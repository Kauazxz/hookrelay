// Package server exposes the HTTP API: webhook ingestion, event inspection,
// replay and health.
package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Kauazxz/hookrelay/internal/store"
	"github.com/Kauazxz/hookrelay/internal/webhook"
)

// Source is the ingestion policy for one webhook producer.
type Source struct {
	Verifier webhook.Verifier
	Keys     webhook.KeySpec
}

// Options configures the server.
type Options struct {
	AdminToken   string // empty disables the admin API
	MaxBodyBytes int64
}

// Server holds the HTTP handlers. Build one with New and mount Handler().
type Server struct {
	store     store.Store
	sources   map[string]Source
	adminHash *[32]byte
	maxBody   int64
	notify    func() // wakes the dispatcher after a new event is stored
	log       *slog.Logger
}

// New returns a Server. notify may be nil.
func New(st store.Store, sources map[string]Source, opts Options, notify func(), log *slog.Logger) *Server {
	s := &Server{store: st, sources: sources, maxBody: opts.MaxBodyBytes, notify: notify, log: log}
	if opts.AdminToken != "" {
		h := sha256.Sum256([]byte(opts.AdminToken))
		s.adminHash = &h
	}
	if s.notify == nil {
		s.notify = func() {}
	}
	return s
}

// Handler returns the routed, logged handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /hooks/{source}", s.ingest)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /events", s.admin(s.listEvents))
	mux.Handle("GET /events/{id}", s.admin(s.getEvent))
	mux.Handle("POST /events/{id}/replay", s.admin(s.replay))
	return s.logRequests(mux)
}

type ingestResponse struct {
	ID        string `json:"id"`
	Duplicate bool   `json:"duplicate"`
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("source")
	src, ok := s.sources[name]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown source")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBody))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "could not read body")
		return
	}
	if err := src.Verifier.Verify(r.Header, body); err != nil {
		s.log.Warn("rejected webhook", "source", name, "reason", err, "remote", r.RemoteAddr)
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	ev, created, err := s.store.Create(r.Context(), store.Event{
		Source:         name,
		IdempotencyKey: src.Keys.Extract(r.Header, body),
		ContentType:    r.Header.Get("Content-Type"),
		Body:           body,
	})
	if err != nil {
		s.log.Error("store event", "source", name, "err", err)
		writeError(w, http.StatusInternalServerError, "could not store event")
		return
	}
	if !created {
		// The producer is re-delivering something we already have. Acknowledge
		// it so it stops retrying, but do not forward it again.
		s.log.Info("duplicate webhook", "source", name, "event_id", ev.ID, "key", ev.IdempotencyKey)
		writeJSON(w, http.StatusOK, ingestResponse{ID: ev.ID, Duplicate: true})
		return
	}
	s.log.Info("accepted webhook", "source", name, "event_id", ev.ID, "key", ev.IdempotencyKey)
	s.notify()
	writeJSON(w, http.StatusAccepted, ingestResponse{ID: ev.ID})
}

// admin guards a handler with "Authorization: Bearer <token>". Comparing
// SHA-256 digests in constant time hides both content and length.
func (s *Server) admin(h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		sum := sha256.Sum256([]byte(tok))
		if s.adminHash == nil || !ok || subtle.ConstantTimeCompare(sum[:], s.adminHash[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="hookrelay"`)
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		h(w, r)
	})
}

// eventView renders the body as JSON when it is JSON, else as a string.
type eventView struct {
	store.Event
	Body any `json:"body,omitempty"` // shadows Event.Body (base64 []byte)
}

func view(e store.Event, withBody bool) eventView {
	v := eventView{Event: e}
	if withBody {
		if json.Valid(e.Body) {
			v.Body = json.RawMessage(e.Body)
		} else {
			v.Body = string(e.Body)
		}
	}
	return v
}

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.Filter{Source: q.Get("source"), Status: store.Status(q.Get("status")), Limit: 50}
	if f.Status != "" && !f.Status.Valid() {
		writeError(w, http.StatusBadRequest, "status must be pending, delivered or dead")
		return
	}
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 || n > 500 {
			writeError(w, http.StatusBadRequest, "limit must be 1..500")
			return
		}
		f.Limit = n
	}
	evs, err := s.store.List(r.Context(), f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]eventView, len(evs))
	for i, e := range evs {
		out[i] = view(e, false)
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": out})
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	ev, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view(ev, true))
}

var errNotDead = errors.New("only dead-lettered events can be replayed")

// replay moves a dead-lettered event back to pending with a fresh attempt
// budget. Status is checked inside Update, so two concurrent replays cannot
// both succeed and a delivered event can never be re-sent this way.
func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	ev, err := s.store.Update(r.Context(), r.PathValue("id"), func(e *store.Event) error {
		if e.Status != store.StatusDead {
			return errNotDead
		}
		e.Status, e.Attempts, e.NextAttemptAt, e.UpdatedAt = store.StatusPending, 0, now, now
		return nil
	})
	if err != nil {
		s.storeError(w, err)
		return
	}
	s.log.Info("event replayed", "event_id", ev.ID, "source", ev.Source)
	s.notify()
	writeJSON(w, http.StatusAccepted, view(ev, false))
}

func (s *Server) storeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, errNotDead):
		writeError(w, http.StatusConflict, err.Error())
	default:
		s.log.Error("store", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.Debug("http request", "method", r.Method, "path", r.URL.Path,
			"status", rec.status, "duration", time.Since(start))
	})
}
