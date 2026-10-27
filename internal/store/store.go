// Package store persists webhook events. The Store interface is the only thing
// the HTTP layer and the dispatcher know about, so a Postgres implementation can
// be added later without touching them.
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

// Status is the delivery state of an event.
type Status string

const (
	StatusPending   Status = "pending"   // waiting for (another) delivery attempt
	StatusDelivered Status = "delivered" // target answered 2xx
	StatusDead      Status = "dead"      // gave up after max attempts; can be replayed
)

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	return s == StatusPending || s == StatusDelivered || s == StatusDead
}

// Event is one accepted webhook delivery.
type Event struct {
	ID             string     `json:"id"`
	Source         string     `json:"source"`
	IdempotencyKey string     `json:"idempotency_key"`
	ContentType    string     `json:"content_type,omitempty"`
	Body           []byte     `json:"body"`
	Status         Status     `json:"status"`
	Attempts       int        `json:"attempts"`
	NextAttemptAt  time.Time  `json:"next_attempt_at"`
	LastError      string     `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	DeliveredAt    *time.Time `json:"delivered_at,omitempty"`
}

// Filter narrows List. Zero values mean "any".
type Filter struct {
	Source string
	Status Status
	Limit  int
}

var (
	// ErrNotFound is returned when no event has the given ID.
	ErrNotFound = errors.New("event not found")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("store closed")
)

// Store is the persistence contract. Implementations must be safe for
// concurrent use and every method must be atomic.
type Store interface {
	// Create inserts e unless an event with the same (Source, IdempotencyKey)
	// already exists. It returns the stored event and whether it was created.
	// A missing ID is generated.
	Create(ctx context.Context, e Event) (Event, bool, error)
	Get(ctx context.Context, id string) (Event, error)
	// List returns matching events, newest first.
	List(ctx context.Context, f Filter) ([]Event, error)
	// Update applies fn to a copy of the event under the store's lock and
	// persists the result. If fn returns an error nothing is written and that
	// error is returned.
	Update(ctx context.Context, id string, fn func(*Event) error) (Event, error)
	// ClaimDue returns up to limit pending events whose NextAttemptAt <= now and
	// pushes their NextAttemptAt to now+lease, so no other worker (or the next
	// process, after a crash) picks them up until the lease expires.
	ClaimDue(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]Event, error)
	Close() error
}

// NewID returns a random, URL-safe event ID.
func NewID() string {
	var b [12]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read does not fail on supported platforms
	return "evt_" + hex.EncodeToString(b[:])
}
