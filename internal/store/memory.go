package store

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"
)

// Memory is an in-process Store. It is also the index behind the file store,
// which sets persist so every change hits disk before it becomes visible.
type Memory struct {
	mu     sync.Mutex
	byID   map[string]*Event
	byKey  map[string]string // source + "\x00" + idempotency key -> event ID
	closed bool

	// persist, when set, durably records events before they are applied.
	// It is called with mu held.
	persist func(...Event) error
}

var _ Store = (*Memory)(nil)

// NewMemory returns an empty in-memory store. Data is lost on restart.
func NewMemory() *Memory {
	return &Memory{byID: map[string]*Event{}, byKey: map[string]string{}}
}

func dedupKey(source, key string) string { return source + "\x00" + key }

// apply puts e into the index. Callers hold mu.
func (m *Memory) apply(e Event) {
	m.byID[e.ID] = &e
	m.byKey[dedupKey(e.Source, e.IdempotencyKey)] = e.ID
}

// commit persists (if configured) and then applies evs. Callers hold mu.
func (m *Memory) commit(evs ...Event) error {
	if m.closed {
		return ErrClosed
	}
	if m.persist != nil {
		if err := m.persist(evs...); err != nil {
			return err
		}
	}
	for _, e := range evs {
		m.apply(e)
	}
	return nil
}

// Create implements Store.
func (m *Memory) Create(_ context.Context, e Event) (Event, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Event{}, false, ErrClosed
	}
	if id, ok := m.byKey[dedupKey(e.Source, e.IdempotencyKey)]; ok {
		return *m.byID[id], false, nil
	}
	now := time.Now().UTC()
	if e.ID == "" {
		e.ID = NewID()
	}
	if e.Status == "" {
		e.Status = StatusPending
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = now
	}
	if e.UpdatedAt.IsZero() {
		e.UpdatedAt = e.CreatedAt
	}
	if e.NextAttemptAt.IsZero() {
		e.NextAttemptAt = e.CreatedAt
	}
	e.Body = bytes.Clone(e.Body)
	if err := m.commit(e); err != nil {
		return Event{}, false, err
	}
	return e, true, nil
}

// Get implements Store.
func (m *Memory) Get(_ context.Context, id string) (Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Event{}, ErrClosed
	}
	e, ok := m.byID[id]
	if !ok {
		return Event{}, ErrNotFound
	}
	return *e, nil
}

// List implements Store. It scans every event, which is fine for the volumes
// this store targets; a database would use an index.
func (m *Memory) List(_ context.Context, f Filter) ([]Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	out := make([]Event, 0)
	for _, e := range m.byID {
		if (f.Source == "" || e.Source == f.Source) && (f.Status == "" || e.Status == f.Status) {
			out = append(out, *e)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].ID > out[j].ID
	})
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

// Update implements Store.
func (m *Memory) Update(_ context.Context, id string, fn func(*Event) error) (Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Event{}, ErrClosed
	}
	cur, ok := m.byID[id]
	if !ok {
		return Event{}, ErrNotFound
	}
	next := *cur
	if err := fn(&next); err != nil {
		return Event{}, err
	}
	// Identity fields are immutable; the dedup index depends on them.
	next.ID, next.Source, next.IdempotencyKey = cur.ID, cur.Source, cur.IdempotencyKey
	if err := m.commit(next); err != nil {
		return Event{}, err
	}
	return next, nil
}

// ClaimDue implements Store.
func (m *Memory) ClaimDue(_ context.Context, now time.Time, lease time.Duration, limit int) ([]Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	var due []Event
	for _, e := range m.byID {
		if e.Status == StatusPending && !e.NextAttemptAt.After(now) {
			due = append(due, *e)
		}
	}
	if len(due) == 0 {
		return nil, nil
	}
	sort.Slice(due, func(i, j int) bool { return due[i].NextAttemptAt.Before(due[j].NextAttemptAt) })
	if limit > 0 && len(due) > limit {
		due = due[:limit]
	}
	for i := range due {
		due[i].NextAttemptAt = now.Add(lease)
	}
	if err := m.commit(due...); err != nil {
		return nil, err
	}
	return due, nil
}

// Close implements Store.
func (m *Memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
