package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
)

// runWithLocks acquires one or more PostgreSQL transaction-level advisory locks
// before calling fn. Each lock ID must be free; if any is already held the
// trigger is dropped immediately (no retry — this is by design).
//
// onAcquired is called immediately after ALL locks are held, before fn starts.
// Use it to record the activity log entry so the entry only exists when the job
// actually runs — never for dropped triggers.
//
// Returns true if all locks were acquired and fn was called,
// false if any lock was busy (trigger dropped silently).
func runWithLocks(ctx context.Context, d *dal.DAL, jobName string, lockIDs []int64, onAcquired func(), fn func(ctx context.Context)) bool {
	span := trace.SpanFromContext(ctx)

	tx, err := d.AcquireAdvisoryLock(ctx)
	if err != nil {
		span.RecordError(err)
		log.Printf("%s: acquire advisory lock tx: %v", jobName, err)
		return false
	}
	defer func() { _ = tx.Rollback() }()

	for _, id := range lockIDs {
		locked, err := tx.QueryBoolContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", id)
		if err != nil {
			span.RecordError(err)
			log.Printf("%s: pg_try_advisory_xact_lock(%d): %v", jobName, id, err)
			return false
		}
		if !locked {
			span.AddEvent(fmt.Sprintf("lock %d busy — %s trigger dropped", id, jobName))
			log.Printf("%s: lock %d busy, trigger dropped", jobName, id)
			return false
		}
	}

	// All locks held — notify caller before starting work.
	if onAcquired != nil {
		onAcquired()
	}

	lockCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Heartbeat: cancel all child work if the direct DB connection drops.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-lockCtx.Done():
				return
			case <-ticker.C:
				hbCtx, hbCancel := context.WithTimeout(lockCtx, 5*time.Second)
				_, hbErr := tx.QueryBoolContext(hbCtx, "SELECT true")
				hbCancel()
				if hbErr != nil {
					span.RecordError(fmt.Errorf("%s: lock heartbeat lost: %w", jobName, hbErr))
					cancel()
					return
				}
			}
		}
	}()

	fn(lockCtx)
	return true
}
