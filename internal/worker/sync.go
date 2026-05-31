package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
)

var syncTracer = otel.Tracer("worker/sync")

// RunSyncHandler processes all pending SyncTask (Outbox) events.
// It acquires LockIDSync to serialise sync runs and prevent concurrent
// ResolvedTickCount recomputes on the same CANDLE rows.
func RunSyncHandler(ctx context.Context, d *dal.DAL) {
	ctx, span := syncTracer.Start(ctx, "RunSyncHandler")
	defer span.End()

	tx, err := d.AcquireAdvisoryLock(ctx)
	if err != nil {
		span.RecordError(err)
		log.Printf("sync: acquire lock: %v", err)
		return
	}
	defer func() { _ = tx.Rollback() }()

	var locked bool
	err = tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", dal.LockIDSync).Scan(&locked)
	if err != nil || !locked {
		span.AddEvent("LockIDSync already held by another instance, skipping")
		return
	}

	lockCtx, cancelETL := context.WithCancel(ctx)
	defer cancelETL()

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-lockCtx.Done():
				return
			case <-ticker.C:
				hbCtx, hbCancel := context.WithTimeout(lockCtx, 5*time.Second)
				var alive bool
				err := tx.QueryRowContext(hbCtx, "SELECT true").Scan(&alive)
				hbCancel()
				if err != nil {
					span.RecordError(fmt.Errorf("sync lock heartbeat lost: %w", err))
					cancelETL()
					return
				}
			}
		}
	}()

	processPendingSyncTasks(lockCtx, d)
}

// processPendingSyncTasks fetches all PENDING SyncTask rows and recomputes
// ResolvedTickCount for the corresponding CANDLE row idempotently.
func processPendingSyncTasks(ctx context.Context, d *dal.DAL) {
	defer recoverGoroutine(ctx, "processPendingSyncTasks")

	// TODO:
	// 1. Query all SyncTask WHERE status=PENDING using FOR UPDATE SKIP LOCKED.
	// 2. For each task call processSyncTask(ctx, task.ID, d).
	//
	// processSyncTask per the Outbox spec:
	//   a. Claim (mark PROCESSED).
	//   b. COUNT states WHERE job_type=TICK AND status=CONFIRMED AND date=targetDate.
	//   c. UPDATE candle SET resolved_tick_count=count [, status=PENDING if count<24].
	//   d. DELETE SyncTask WHERE id=task.ID AND status=PROCESSED (conditional delete).
	log.Println("sync: not yet implemented")
}
