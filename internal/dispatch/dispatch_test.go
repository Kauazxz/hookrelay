package dispatch

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Kauazxz/hookrelay/internal/store"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func fastOpts(maxAttempts int) Options {
	return Options{
		Workers: 2, PollInterval: 5 * time.Millisecond, RequestTimeout: 2 * time.Second,
		MaxAttempts: maxAttempts, BackoffBase: 5 * time.Millisecond, BackoffMax: 20 * time.Millisecond,
	}
}

// start runs a dispatcher for target until the test ends.
func start(t *testing.T, st store.Store, target string, opts Options) *Dispatcher {
	t.Helper()
	d := New(st, map[string]string{"pay": target}, opts, quiet)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return d
}

func waitStatus(t *testing.T, st store.Store, id string, want store.Status) store.Event {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		e, err := st.Get(context.Background(), id)
		if err == nil && e.Status == want {
			return e
		}
		if time.Now().After(deadline) {
			t.Fatalf("event %s: status %q (attempts %d, last error %q), want %q", id, e.Status, e.Attempts, e.LastError, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRetriesWithBackoffUntilSuccess(t *testing.T) {
	var mu sync.Mutex
	var attempts []string
	var at []time.Time
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, r.Header.Get("X-Hookrelay-Attempt"))
		at = append(at, time.Now())
		if len(attempts) < 3 {
			http.Error(w, "temporarily down", http.StatusServiceUnavailable)
			return
		}
		if b, _ := io.ReadAll(r.Body); string(b) != `{"order":42}` || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("forwarded body/content-type changed: %q %q", b, r.Header.Get("Content-Type"))
		}
	}))
	defer target.Close()

	st := store.NewMemory()
	e, _, _ := st.Create(context.Background(), store.Event{Source: "pay", IdempotencyKey: "k", ContentType: "application/json", Body: []byte(`{"order":42}`)})
	start(t, st, target.URL, fastOpts(5))

	got := waitStatus(t, st, e.ID, store.StatusDelivered)
	if got.Attempts != 3 || got.DeliveredAt == nil || got.LastError != "" {
		t.Fatalf("delivered event: %+v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(attempts, ",") != "1,2,3" {
		t.Fatalf("attempt headers = %v", attempts)
	}
	// Equal jitter never waits less than half the nominal delay: 2.5ms, then 5ms.
	if gap := at[1].Sub(at[0]); gap < 2*time.Millisecond {
		t.Fatalf("first retry came after %v, backoff not applied", gap)
	}
	if gap := at[2].Sub(at[1]); gap < 5*time.Millisecond {
		t.Fatalf("second retry came after %v, backoff did not grow", gap)
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	var hits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer target.Close()

	st := store.NewMemory()
	e, _, _ := st.Create(context.Background(), store.Event{Source: "pay", IdempotencyKey: "k"})
	start(t, st, target.URL, fastOpts(3))

	got := waitStatus(t, st, e.ID, store.StatusDead)
	if got.Attempts != 3 || !strings.Contains(got.LastError, "500") {
		t.Fatalf("dead event: attempts=%d last_error=%q", got.Attempts, got.LastError)
	}
	time.Sleep(50 * time.Millisecond)
	if n := hits.Load(); n != 3 {
		t.Fatalf("target hit %d times, want exactly 3", n)
	}
}

func TestShutdownDrainsInFlightDelivery(t *testing.T) {
	received := make(chan struct{})
	release := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(received)
		<-release
	}))
	defer target.Close()

	st := store.NewMemory()
	e, _, _ := st.Create(context.Background(), store.Event{Source: "pay", IdempotencyKey: "k"})
	d := New(st, map[string]string{"pay": target.URL}, fastOpts(3), quiet)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()

	<-received
	cancel() // SIGTERM while the target is still answering
	select {
	case <-done:
		t.Fatal("Run returned before the in-flight delivery finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-done
	if got, _ := st.Get(context.Background(), e.ID); got.Status != store.StatusDelivered {
		t.Fatalf("in-flight delivery not recorded: %+v", got)
	}
}

func TestBackoff(t *testing.T) {
	s := time.Second
	cases := []struct {
		n    int
		rnd  float64
		want time.Duration
	}{
		{1, 0, 500 * time.Millisecond}, // lower bound is half the nominal delay
		{1, 0.99, 995 * time.Millisecond},
		{3, 0, 2 * s},   // 1s * 2^2 = 4s, halved
		{4, 0.5, 6 * s}, // 8s nominal: 4s + 0.5*4s
		{50, 0, 5 * s},  // capped at 10s; a huge n does not overflow
		{50, 0.99, 9950 * time.Millisecond},
	}
	for _, tc := range cases {
		if got := Backoff(tc.n, s, 10*s, tc.rnd); got != tc.want {
			t.Errorf("Backoff(%d, rnd=%v) = %v, want %v", tc.n, tc.rnd, got, tc.want)
		}
	}
}
