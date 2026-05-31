package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/ent/synctask"
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
		log.Printf("sync: acquire lock (traceID=%s): %v", span.SpanContext().TraceID(), err)
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

// processPendingSyncTasks fetches the IDs of all PENDING SyncTask rows in a
// single read, then processes each task individually in its own transaction.
func processPendingSyncTasks(ctx context.Context, d *dal.DAL) {
	defer recoverGoroutine(ctx, "processPendingSyncTasks")

	span := trace.SpanFromContext(ctx)

	// Collect all PENDING task IDs in one read so that each subsequent
	// processSingleSyncTask call can open a short-lived write transaction
	// without holding a long query open.
	var ids []uuid.UUID
	err := d.ExecuteInPool(ctx, func(etx *ent.Tx) error {
		var e error
		ids, e = etx.SyncTask.Query().
			Where(synctask.StatusEQ(synctask.StatusPENDING)).
			Order(synctask.ByCreatedAt()).
			IDs(ctx)
		return e
	})
	if err != nil {
		span.RecordError(err)
		log.Printf("sync: query pending tasks (traceID=%s): %v", span.SpanContext().TraceID(), err)
		return
	}

	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		processSingleSyncTask(ctx, d, id)
	}
}

// processSingleSyncTask implements the five-step Outbox pattern for one
// SyncTask row, all within a single ExecuteInPool transaction:
//
//  1. Re-query the row for PENDING status (duplicate-guard).
//  2. Mark the task as PROCESSED.
//  3. COUNT TICK/CONFIRMED state rows whose timestamp falls in [targetDate, targetDate+24h).
//  4. UPDATE the CANDLE state: set resolved_tick_count=actualCount; if actualCount < 24
//     also demote the CANDLE back to PENDING so it is re-aggregated.
//  5. Conditionally DELETE the SyncTask only if its status is still PROCESSED
//     (a concurrent event may have already reset it back to PENDING).
func processSingleSyncTask(ctx context.Context, d *dal.DAL, taskID uuid.UUID) {
	ctx, span := syncTracer.Start(ctx, "sync/process-task",
		trace.WithAttributes(
			attribute.String("task.id", taskID.String()),
		),
	)
	defer span.End()

	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		// Step 1: Re-query for PENDING — skip if already claimed by another runner.
		task, err := tx.SyncTask.Query().
			Where(
				synctask.IDEQ(taskID),
				synctask.StatusEQ(synctask.StatusPENDING),
			).
			Only(ctx)
		if err != nil {
			if ent.IsNotFound(err) {
				// Already processed or re-triggered; nothing to do.
				return nil
			}
			return fmt.Errorf("sync re-query task %s: %w", taskID, err)
		}

		// Step 2: Mark as PROCESSED.
		if err := tx.SyncTask.UpdateOneID(task.ID).
			SetStatus(synctask.StatusPROCESSED).
			Exec(ctx); err != nil {
			return fmt.Errorf("sync mark PROCESSED task %s: %w", taskID, err)
		}

		// Step 3: COUNT TICK/CONFIRMED rows in [targetDate, targetDate+24h).
		windowStart := task.TargetDate
		windowEnd := task.TargetDate.Add(24 * time.Hour)

		actualCount, err := tx.State.Query().
			Where(
				state.InstrumentID(task.InstrumentID),
				state.JobTypeEQ(state.JobTypeTICK),
				state.StatusEQ(state.StatusCONFIRMED),
				state.TimestampGTE(windowStart),
				state.TimestampLT(windowEnd),
			).
			Count(ctx)
		if err != nil {
			return fmt.Errorf("sync count ticks for instrument %s date %s: %w",
				task.InstrumentID, task.TargetDate.Format(time.DateOnly), err)
		}

		// Step 4: UPDATE the CANDLE row — always write the fresh count.
		// If actualCount < 24 the CANDLE is also demoted back to PENDING so
		// the aggregation job picks it up again on the next cycle.
		candleUpdate := tx.State.Update().
			Where(
				state.InstrumentID(task.InstrumentID),
				state.JobTypeEQ(state.JobTypeCANDLE),
				state.TimestampEQ(windowStart),
			).
			SetResolvedTickCount(actualCount).
			SetUpdatedAt(time.Now().UTC())

		if actualCount < 24 {
			candleUpdate = candleUpdate.SetStatus(state.StatusPENDING)
		}

		if err := candleUpdate.Exec(ctx); err != nil {
			return fmt.Errorf("sync update candle for instrument %s date %s: %w",
				task.InstrumentID, task.TargetDate.Format(time.DateOnly), err)
		}

		// Step 5: Conditional DELETE — only removes the row when it is still
		// PROCESSED.  If a concurrent CONFIRMED/BROKEN event has already reset
		// it back to PENDING, the delete predicate will match zero rows and the
		// task will be picked up again in the next RunSyncHandler invocation.
		if _, err := tx.SyncTask.Delete().
			Where(
				synctask.IDEQ(task.ID),
				synctask.StatusEQ(synctask.StatusPROCESSED),
			).
			Exec(ctx); err != nil {
			return fmt.Errorf("sync conditional delete task %s: %w", taskID, err)
		}

		return nil
	})
	if err != nil {
		span.RecordError(err)
		log.Printf("sync: process task %s (traceID=%s): %v", taskID, span.SpanContext().TraceID(), err)
	}
}
