package worker

import (
	"context"
	"fmt"
	"log"
	"time"

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
// It holds the global advisory lock so sync cannot overlap with ticks,
// candles, or maintenance. If the lock is already held the trigger is dropped.
func RunSyncHandler(ctx context.Context, d *dal.DAL, onStarted func()) bool {
	ctx, span := syncTracer.Start(ctx, "RunSyncHandler")
	defer span.End()

	return runWithLocks(ctx, d, "sync", []int64{dal.LockIDSync}, onStarted, func(lockCtx context.Context) {
		processPendingSyncTasks(lockCtx, d)
	})
}

// processPendingSyncTasks fetches the IDs of all PENDING SyncTask rows in a
// SKIP LOCKED batch, then processes each claimed task without re-reading it.
func processPendingSyncTasks(ctx context.Context, d *dal.DAL) {
	defer recoverGoroutine(ctx, "processPendingSyncTasks")

	span := trace.SpanFromContext(ctx)

	for {
		if ctx.Err() != nil {
			return
		}
		processed, err := processOneSyncTask(ctx, d)
		if err != nil {
			span.RecordError(err)
			log.Printf("sync: process task (traceID=%s): %v", span.SpanContext().TraceID(), err)
			return
		}
		if !processed {
			return
		}
	}
}

func claimOneSyncTaskInTx(ctx context.Context, tx *ent.Tx) (*ent.SyncTask, error) {
	rows, err := tx.QueryContext(ctx, `
			WITH claimed AS (
				SELECT id
				FROM ingestion.sync_tasks
				WHERE status = $1
				ORDER BY created_at
				FOR UPDATE SKIP LOCKED
				LIMIT 1
			)
			UPDATE ingestion.sync_tasks AS st
			SET status = $2
			FROM claimed
			WHERE st.id = claimed.id
			RETURNING st.id, st.instrument_id, st.target_date, st.status, st.created_at
		`,
		string(synctask.StatusPENDING),
		string(synctask.StatusPROCESSED),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, rows.Err()
	}
	task := &ent.SyncTask{}
	var status string
	if err := rows.Scan(&task.ID, &task.InstrumentID, &task.TargetDate, &status, &task.CreatedAt); err != nil {
		return nil, err
	}
	task.Status = synctask.Status(status)
	return task, rows.Err()
}

// processOneSyncTask claims one SyncTask and completes the recompute in the same
// transaction scope.
func processOneSyncTask(ctx context.Context, d *dal.DAL) (bool, error) {
	ctx, span := syncTracer.Start(ctx, "sync/process-task")
	defer span.End()

	processed := false
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		task, err := claimOneSyncTaskInTx(ctx, tx)
		if err != nil || task == nil {
			return err
		}
		processed = true
		span.SetAttributes(attribute.String("task.id", task.ID.String()))
		log.Printf("sync: processing task %s for instrument %s targetDate %s", task.ID, task.InstrumentID, task.TargetDate.Format(time.DateOnly))

		// COUNT TICK/CONFIRMED rows in [targetDate, targetDate+24h).
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

		// UPDATE the CANDLE row — always write the fresh count.
		// If actualCount < 24, the CANDLE is also demoted back to PENDING, so
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
		log.Printf("sync: completed task %s - candle %s actual count: %d", task.ID, task.TargetDate.Format(time.DateOnly), actualCount)

		// Conditional DELETE — only removes the row when it is still
		// PROCESSED.  If a concurrent CONFIRMED/BROKEN event has already reset
		// it back to PENDING, the delete predicate will match zero rows and the
		// task will be picked up again in the next RunSyncHandler invocation.
		if _, err := tx.SyncTask.Delete().
			Where(
				synctask.IDEQ(task.ID),
				synctask.StatusEQ(synctask.StatusPROCESSED),
			).
			Exec(ctx); err != nil {
			return fmt.Errorf("sync conditional delete task %s: %w", task.ID, err)
		}

		return nil
	})
	return processed, err
}
