package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Kauazxz/hookrelay/internal/dispatch"
	"github.com/Kauazxz/hookrelay/internal/store"
	"github.com/Kauazxz/hookrelay/internal/webhook"
)

const (
	secret = "whsec_test"
	token  = "admin-token"
)

type env struct {
	relay   *httptest.Server
	store   store.Store
	hits    atomic.Int32 // requests the target received
	healthy atomic.Bool  // target answers 200 when true, 500 otherwise
}

// newEnv wires store, dispatcher, API and a fake target, like main does.
func newEnv(t *testing.T, maxAttempts int) *env {
	t.Helper()
	e := &env{store: store.NewMemory()}
	e.healthy.Store(true)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		e.hits.Add(1)
		if !e.healthy.Load() {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(target.Close)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	disp := dispatch.New(e.store, map[string]string{"pay": target.URL}, dispatch.Options{
		Workers: 4, PollInterval: 5 * time.Millisecond, RequestTimeout: 2 * time.Second,
		MaxAttempts: maxAttempts, BackoffBase: time.Millisecond, BackoffMax: 5 * time.Millisecond,
	}, log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { disp.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	srv := New(e.store, map[string]Source{"pay": {
		Verifier: webhook.Verifier{Secret: []byte(secret), Header: "X-Signature", Prefix: "sha256="},
		Keys:     webhook.KeySpec{Header: "Idempotency-Key", Field: "id"},
	}}, Options{AdminToken: token, MaxBodyBytes: 1024}, disp.Notify, log)
	e.relay = httptest.NewServer(srv.Handler())
	t.Cleanup(e.relay.Close)
	return e
}

func (e *env) do(t *testing.T, method, path, bearer string, body []byte, hdr map[string]string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, e.relay.URL+path, bytes.NewReader(body))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (e *env) post(t *testing.T, body string, hdr map[string]string) (int, map[string]any) {
	t.Helper()
	h := map[string]string{
		"Content-Type": "application/json",
		"X-Signature":  "sha256=" + hex.EncodeToString(webhook.Sign([]byte(secret), "", []byte(body))),
	}
	for k, v := range hdr {
		h[k] = v
	}
	return e.do(t, http.MethodPost, "/hooks/pay", "", []byte(body), h)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func TestIngestRejectsBadRequests(t *testing.T) {
	e := newEnv(t, 3)
	if code, _ := e.post(t, `{"id":"a"}`, map[string]string{"X-Signature": "sha256=00ff"}); code != http.StatusUnauthorized {
		t.Fatalf("bad signature: got %d", code)
	}
	if code, _ := e.post(t, `{"id":"a"}`, map[string]string{"X-Signature": ""}); code != http.StatusUnauthorized {
		t.Fatalf("missing signature: got %d", code)
	}
	if code, _ := e.post(t, `{"id":"`+strings.Repeat("x", 2048)+`"}`, nil); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: got %d", code)
	}
	if code, _ := e.do(t, http.MethodPost, "/hooks/unknown", "", nil, nil); code != http.StatusNotFound {
		t.Fatalf("unknown source: got %d", code)
	}
	if code, _ := e.do(t, http.MethodGet, "/hooks/pay", "", nil, nil); code != http.StatusMethodNotAllowed {
		t.Fatalf("GET on hook: got %d", code)
	}
	if all, _ := e.store.List(context.Background(), store.Filter{}); len(all) != 0 {
		t.Fatalf("rejected requests were stored: %d", len(all))
	}
}

func TestDuplicatesAreAcknowledgedButForwardedOnce(t *testing.T) {
	e := newEnv(t, 3)

	code, first := e.post(t, `{"id":"pay_1","status":"paid"}`, nil)
	if code != http.StatusAccepted || first["duplicate"] != false {
		t.Fatalf("first delivery: %d %v", code, first)
	}
	// The provider re-sends the same payment (key taken from the JSON "id").
	code, again := e.post(t, `{"id":"pay_1","status":"paid","retry":1}`, nil)
	if code != http.StatusOK || again["duplicate"] != true || again["id"] != first["id"] {
		t.Fatalf("re-delivery: %d %v", code, again)
	}

	// 20 concurrent copies of a new event: exactly one is accepted.
	var wg sync.WaitGroup
	var accepted, dups atomic.Int32
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch code, body := e.post(t, `{"id":"pay_2"}`, map[string]string{"Idempotency-Key": "pay_2"}); {
			case code == http.StatusAccepted:
				accepted.Add(1)
			case code == http.StatusOK && body["duplicate"] == true:
				dups.Add(1)
			default:
				t.Errorf("concurrent post: %d %v", code, body)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 || dups.Load() != 19 {
		t.Fatalf("accepted=%d duplicates=%d, want 1 and 19", accepted.Load(), dups.Load())
	}

	eventually(t, "both events delivered", func() bool { return e.hits.Load() == 2 })
	time.Sleep(50 * time.Millisecond)
	if n := e.hits.Load(); n != 2 {
		t.Fatalf("target received %d requests, want 2", n)
	}
}

func TestAdminRequiresBearerToken(t *testing.T) {
	e := newEnv(t, 3)
	for _, bearer := range []string{"", "wrong"} {
		if code, _ := e.do(t, http.MethodGet, "/events", bearer, nil, nil); code != http.StatusUnauthorized {
			t.Fatalf("token %q: got %d", bearer, code)
		}
	}
	if code, _ := e.do(t, http.MethodGet, "/events", token, nil, nil); code != http.StatusOK {
		t.Fatalf("valid token: got %d", code)
	}
	if code, _ := e.do(t, http.MethodGet, "/healthz", "", nil, nil); code != http.StatusOK {
		t.Fatalf("healthz: got %d", code)
	}
}

func TestReplayDeadLetteredEvent(t *testing.T) {
	e := newEnv(t, 2)
	e.healthy.Store(false)

	_, res := e.post(t, `{"id":"pay_9"}`, nil)
	id := res["id"].(string)
	eventually(t, "dead letter", func() bool {
		ev, _ := e.store.Get(context.Background(), id)
		return ev.Status == store.StatusDead
	})
	code, list := e.do(t, http.MethodGet, "/events?source=pay&status=dead", token, nil, nil)
	if evs, _ := list["events"].([]any); code != http.StatusOK || len(evs) != 1 {
		t.Fatalf("list dead: %d %v", code, list)
	}

	if code, _ := e.do(t, http.MethodPost, "/events/evt_nope/replay", token, nil, nil); code != http.StatusNotFound {
		t.Fatalf("replay unknown: got %d", code)
	}
	e.healthy.Store(true)
	if code, body := e.do(t, http.MethodPost, "/events/"+id+"/replay", token, nil, nil); code != http.StatusAccepted || body["status"] != "pending" {
		t.Fatalf("replay: %d %v", code, body)
	}
	eventually(t, "replayed event delivered", func() bool {
		ev, _ := e.store.Get(context.Background(), id)
		return ev.Status == store.StatusDelivered
	})
	if code, _ := e.do(t, http.MethodPost, "/events/"+id+"/replay", token, nil, nil); code != http.StatusConflict {
		t.Fatalf("replay of delivered event: got %d, want 409", code)
	}

	code, ev := e.do(t, http.MethodGet, "/events/"+id, token, nil, nil)
	body, _ := ev["body"].(map[string]any)
	if code != http.StatusOK || ev["attempts"] != float64(1) || body["id"] != "pay_9" {
		t.Fatalf("get event: %d %v", code, ev)
	}
}
