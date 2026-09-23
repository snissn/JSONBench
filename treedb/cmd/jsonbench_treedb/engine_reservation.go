package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/collections"
)

// enginePrepareReservation controls batch-owned capacity across the producer
// and ordered committer. The charges supplied by the engine still need their
// own proven transient bounds before this becomes a strict memory limit.
type enginePrepareReservation struct {
	mu      sync.Mutex
	limit   int64
	used    int64
	maximum int64
	changed chan struct{}
}

func newEnginePrepareReservation(limit int64) *enginePrepareReservation {
	return &enginePrepareReservation{limit: limit, changed: make(chan struct{})}
}

// acquire reserves only the requested source backing before it can grow.
func (q *enginePrepareReservation) acquire(ctx context.Context, amount int64) error {
	if amount <= 0 || amount > q.limit {
		return fmt.Errorf("invalid engine reservation request %d against limit %d", amount, q.limit)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		q.mu.Lock()
		available := q.limit - q.used
		if available >= amount {
			q.used += amount
			if q.used > q.maximum {
				q.maximum = q.used
			}
			q.mu.Unlock()
			return nil
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// acquireAvailable gives preparation the currently free credit. Source-only
// credit is kept separately so a committer can overlap the next source batch.
func (q *enginePrepareReservation) acquireAvailable(ctx context.Context, leave, own int64) (int64, bool, error) {
	if leave < 0 || leave >= q.limit {
		return 0, false, fmt.Errorf("invalid engine reservation source slot %d against limit %d", leave, q.limit)
	}
	for {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		q.mu.Lock()
		available := q.limit - q.used - leave
		if available > 0 {
			hadOther := q.used > own
			q.used += available
			if q.used > q.maximum {
				q.maximum = q.used
			}
			q.mu.Unlock()
			return available, hadOther, nil
		}
		if q.used == own {
			q.mu.Unlock()
			return 0, false, fmt.Errorf("%w: source owner %d and successor slot %d exceed limit %d", collections.ErrPreparedInsertResourceLimit, own, leave, q.limit)
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return 0, false, ctx.Err()
		}
	}
}

// waitForOtherOwner waits after a budget rejection that may be caused by the
// predecessor's still-live reservation. The caller releases temporary prep
// credit first, preserving the ordered committer's already admitted headroom.
func (q *enginePrepareReservation) waitForOtherOwner(ctx context.Context, own int64) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		q.mu.Lock()
		other := q.used > own
		changed := q.changed
		q.mu.Unlock()
		if !other {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (q *enginePrepareReservation) shrink(before, after int64) {
	if after < 0 || after > before {
		panic(fmt.Sprintf("invalid engine reservation shrink: before=%d after=%d", before, after))
	}
	q.release(before - after)
}

func (q *enginePrepareReservation) release(amount int64) {
	if amount <= 0 {
		return
	}
	q.mu.Lock()
	if amount > q.used {
		used := q.used
		q.mu.Unlock()
		panic(fmt.Sprintf("engine reservation release %d exceeds used %d", amount, used))
	}
	q.used -= amount
	close(q.changed)
	q.changed = make(chan struct{})
	q.mu.Unlock()
}

func (q *enginePrepareReservation) peak() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.maximum
}
