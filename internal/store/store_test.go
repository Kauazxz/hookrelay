package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var ctx = context.Background()

// stores runs fn against every implementation.
func stores(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Run("memory", func(t *testing.T) { fn(t, NewMemory()) })
	t.Run("file", func(t *testing.T) {
		s, err := OpenFile(filepath.Join(t.TempDir(), "events.jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		fn(t, s)
	})
}

func TestCreateDeduplicates(t *testing.T) {
	stores(t, func(t *testing.T, s Store) {
		first, created, err := s.Create(ctx, Event{Source: "pay", IdempotencyKey: "k1", Body: []byte(`{}`)})
		if err != nil || !created {
			t.Fatalf("first create: created=%v err=%v", created, err)
		}
		again, created, err := s.Create(ctx, Event{Source: "pay", IdempotencyKey: "k1", Body: []byte(`{"x":1}`)})
		if err != nil || created || again.ID != first.ID {
			t.Fatalf("duplicate: created=%v id=%s want %s err=%v", created, again.ID, first.ID, err)
		}
		// Same key under another source is a different event.
		if _, created, _ := s.Create(ctx, Event{Source: "shop", IdempotencyKey: "k1"}); !created {
			t.Fatal("key must be scoped per source")
		}
	})
}

func TestConcurrentCreateStoresOnce(t *testing.T) {
	stores(t, func(t *testing.T, s Store) {
		const n = 50
		var wg sync.WaitGroup
		var mu sync.Mutex
		created, ids := 0, map[string]bool{}
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				e, ok, err := s.Create(ctx, Event{Source: "pay", IdempotencyKey: "same"})
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				ids[e.ID] = true
				if ok {
					created++
				}
			}()
		}
		wg.Wait()
		if created != 1 || len(ids) != 1 {
			t.Fatalf("created=%d distinct ids=%d, want 1 and 1", created, len(ids))
		}
	})
}

func TestClaimDueLeasesEvents(t *testing.T) {
	stores(t, func(t *testing.T, s Store) {
		now := time.Now().UTC()
		e, _, _ := s.Create(ctx, Event{Source: "pay", IdempotencyKey: "a", NextAttemptAt: now})
		s.Create(ctx, Event{Source: "pay", IdempotencyKey: "later", NextAttemptAt: now.Add(time.Hour)})

		got, err := s.ClaimDue(ctx, now, time.Minute, 10)
		if err != nil || len(got) != 1 || got[0].ID != e.ID {
			t.Fatalf("claim: %v %v", got, err)
		}
		if again, _ := s.ClaimDue(ctx, now, time.Minute, 10); len(again) != 0 {
			t.Fatalf("leased event claimed twice: %v", again)
		}
		if after, _ := s.ClaimDue(ctx, now.Add(2*time.Minute), time.Minute, 10); len(after) != 1 {
			t.Fatalf("expired lease should be claimable again, got %d", len(after))
		}
	})
}

func TestUpdateAndList(t *testing.T) {
	stores(t, func(t *testing.T, s Store) {
		e, _, _ := s.Create(ctx, Event{Source: "pay", IdempotencyKey: "a"})
		s.Create(ctx, Event{Source: "shop", IdempotencyKey: "b"})

		boom := errors.New("boom")
		if _, err := s.Update(ctx, e.ID, func(*Event) error { return boom }); !errors.Is(err, boom) {
			t.Fatalf("fn error not returned: %v", err)
		}
		if _, err := s.Update(ctx, e.ID, func(ev *Event) error { ev.Status = StatusDead; return nil }); err != nil {
			t.Fatal(err)
		}
		dead, _ := s.List(ctx, Filter{Status: StatusDead})
		if len(dead) != 1 || dead[0].ID != e.ID {
			t.Fatalf("filter by status: %v", dead)
		}
		shop, _ := s.List(ctx, Filter{Source: "shop"})
		if len(shop) != 1 || shop[0].Source != "shop" {
			t.Fatalf("filter by source: %v", shop)
		}
		if _, err := s.Get(ctx, "evt_missing"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})
}

func TestFileStoreSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	s, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	e, _, _ := s.Create(ctx, Event{Source: "pay", IdempotencyKey: "order-42", Body: []byte(`{"amount":10}`)})
	s.Update(ctx, e.ID, func(ev *Event) error { ev.Status, ev.Attempts = StatusDelivered, 2; return nil })
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-write: a torn, newline-less line at the end.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString(`{"id":"evt_torn","sou`)
	f.Close()

	s2, err := OpenFile(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	got, err := s2.Get(ctx, e.ID)
	if err != nil || got.Status != StatusDelivered || got.Attempts != 2 || string(got.Body) != `{"amount":10}` {
		t.Fatalf("after restart: %+v err=%v", got, err)
	}
	if _, created, _ := s2.Create(ctx, Event{Source: "pay", IdempotencyKey: "order-42"}); created {
		t.Fatal("dedup index was not rebuilt")
	}
	if _, err := s2.Get(ctx, "evt_torn"); !errors.Is(err, ErrNotFound) {
		t.Fatal("torn line should have been dropped")
	}
	// The torn tail was truncated, so new appends stay parseable.
	s2.Create(ctx, Event{Source: "pay", IdempotencyKey: "order-43"})
	s2.Close()
	s3, err := OpenFile(path)
	if err != nil {
		t.Fatalf("third open: %v", err)
	}
	defer s3.Close()
	if all, _ := s3.List(ctx, Filter{}); len(all) != 2 {
		t.Fatalf("want 2 events, got %d", len(all))
	}
}

func TestFileStoreRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	os.WriteFile(path, []byte("not json\n"), 0o600)
	if _, err := OpenFile(path); err == nil {
		t.Fatal("expected corruption error")
	}
}
