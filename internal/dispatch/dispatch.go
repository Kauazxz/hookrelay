// Package dispatch forwards stored events to their targets with retries,
// exponential backoff with jitter and a dead-letter state.
package dispatch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Kauazxz/hookrelay/internal/store"
)

// Options tunes the dispatcher. All fields are required.
type Options struct {
	Workers        int
	PollInterval   time.Duration // fallback scan for due retries
	RequestTimeout time.Duration
	MaxAttempts    int
	BackoffBase    time.Duration
	BackoffMax     time.Duration
}

// Dispatcher pulls due events from the store and POSTs them to the target
// configured for their source. Delivery is at-least-once: a crash between a
// successful POST and recording it means the event is sent again after its
// lease expires, so targets should de-duplicate on X-Hookrelay-Event-Id.
type Dispatcher struct {
	store   store.Store
	targets map[string]string // source -> target URL
	opts    Options
	client  *http.Client
	log     *slog.Logger
	wake    chan struct{}

	now    func() time.Time
	jitter func() float64 // in [0,1); replaced in tests
}

// New builds a dispatcher. Call Run to start it.
func New(st store.Store, targets map[string]string, opts Options, log *slog.Logger) *Dispatcher {
	return &Dispatcher{
		store:   st,
		targets: targets,
		opts:    opts,
		client:  &http.Client{Timeout: opts.RequestTimeout},
		log:     log,
		wake:    make(chan struct{}, 1),
		now:     func() time.Time { return time.Now().UTC() },
		jitter:  rand.Float64,
	}
}

// Notify wakes the dispatcher so a new event goes out without waiting for the
// next poll. It never blocks.
func (d *Dispatcher) Notify() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// lease is how long a claimed event is hidden from other claims. A claimed
// event can wait up to one RequestTimeout for a free worker and then take one
// more to deliver, so three timeouts leave headroom.
func (d *Dispatcher) lease() time.Duration { return 3 * d.opts.RequestTimeout }

// Run delivers events until ctx is cancelled, then stops claiming, lets every
// in-flight delivery finish and record its result, and returns.
func (d *Dispatcher) Run(ctx context.Context) {
	jobs := make(chan store.Event)
	var wg sync.WaitGroup
	for range d.opts.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range jobs {
				d.deliver(e)
			}
		}()
	}
	defer func() { close(jobs); wg.Wait() }()

	ticker := time.NewTicker(d.opts.PollInterval)
	defer ticker.Stop()
	for {
		d.dispatchDue(ctx, jobs)
		select {
		case <-ctx.Done():
			d.log.Info("dispatcher draining")
			return
		case <-ticker.C:
		case <-d.wake:
		}
	}
}

func (d *Dispatcher) dispatchDue(ctx context.Context, jobs chan<- store.Event) {
	evs, err := d.store.ClaimDue(ctx, d.now(), d.lease(), d.opts.Workers)
	if err != nil {
		d.log.Error("claim due events", "err", err)
		return
	}
	for i, e := range evs {
		select {
		case jobs <- e:
		case <-ctx.Done():
			// Hand unstarted claims back so the next process does not have to
			// wait for their lease to expire.
			for _, rest := range evs[i:] {
				_, _ = d.store.Update(context.Background(), rest.ID, func(ev *store.Event) error {
					ev.NextAttemptAt = d.now()
					return nil
				})
			}
			return
		}
	}
}

var errStale = errors.New("event changed while delivering")

func (d *Dispatcher) deliver(e store.Event) {
	attempt := e.Attempts + 1
	log := d.log.With("event_id", e.ID, "source", e.Source, "attempt", attempt)
	sendErr := d.send(e, attempt)
	now := d.now()

	updated, err := d.store.Update(context.Background(), e.ID, func(ev *store.Event) error {
		if ev.Status != store.StatusPending || ev.Attempts != e.Attempts {
			return errStale // our lease expired and another worker handled it
		}
		ev.Attempts, ev.UpdatedAt = attempt, now
		switch {
		case sendErr == nil:
			ev.Status, ev.LastError, ev.DeliveredAt = store.StatusDelivered, "", &now
		case attempt >= d.opts.MaxAttempts:
			ev.Status, ev.LastError = store.StatusDead, sendErr.Error()
		default:
			ev.LastError = sendErr.Error()
			ev.NextAttemptAt = now.Add(Backoff(attempt, d.opts.BackoffBase, d.opts.BackoffMax, d.jitter()))
		}
		return nil
	})
	switch {
	case errors.Is(err, errStale):
		log.Warn("delivery result discarded", "err", err)
	case err != nil:
		// The lease will expire and the event will be retried: at-least-once.
		log.Error("record delivery result", "err", err)
	case updated.Status == store.StatusDelivered:
		log.Info("delivered")
	case updated.Status == store.StatusDead:
		log.Error("dead-lettered", "err", sendErr)
	default:
		log.Warn("delivery failed, will retry", "err", sendErr, "next_attempt_at", updated.NextAttemptAt)
	}
}

func (d *Dispatcher) send(e store.Event, attempt int) error {
	target, ok := d.targets[e.Source]
	if !ok {
		return fmt.Errorf("no target configured for source %q", e.Source)
	}
	// Not derived from Run's ctx: on shutdown we let the request finish rather
	// than abort it halfway and leave the outcome unknown.
	ctx, cancel := context.WithTimeout(context.Background(), d.opts.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(e.Body))
	if err != nil {
		return err
	}
	if e.ContentType != "" {
		req.Header.Set("Content-Type", e.ContentType)
	}
	req.Header.Set("User-Agent", "hookrelay")
	req.Header.Set("X-Hookrelay-Event-Id", e.ID)
	req.Header.Set("X-Hookrelay-Source", e.Source)
	req.Header.Set("X-Hookrelay-Idempotency-Key", e.IdempotencyKey)
	req.Header.Set("X-Hookrelay-Attempt", strconv.Itoa(attempt))

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) // lets the connection be reused
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("target returned %d: %s", resp.StatusCode, bytes.TrimSpace(snippet))
	}
	return nil
}

// Backoff returns the wait before the retry that follows attempt n (1-based):
// base*2^(n-1) capped at ceiling, with "equal jitter" so the result is uniform in
// [d/2, d). Jitter spreads retries from many events that failed together,
// so a recovering target is not hit by a synchronized wave. rnd is in [0,1).
func Backoff(n int, base, ceiling time.Duration, rnd float64) time.Duration {
	d := base
	for i := 1; i < n && d < ceiling; i++ {
		d *= 2
	}
	if d > ceiling {
		d = ceiling
	}
	half := d / 2
	return half + time.Duration(rnd*float64(d-half))
}
