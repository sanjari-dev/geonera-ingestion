package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"entgo.io/ent/dialect/sql"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/ent/synctask"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
)

var syncTracer = otel.Tracer("worker/sync")

// syncBatchSize is the maximum number of SyncTask rows claimed per trigger.
const syncBatchSize = 12

// RunSyncHandler claims up to syncBatchSize PENDING SyncTask rows ordered by
// hours_count DESC (closest to 24 processed first) and dispatches one goroutine
// per row as fire-and-forget. The handler returns immediately after launching
// goroutines — it does NOT hold a global advisory lock.
// Each goroutine acquires LockIDSync (1004) only when hours_count reaches 24
// and a CANDLE row needs to be updated.
func RunSyncHandler(ctx context.Context, d *dal.DAL, onStarted func()) bool {
	ctx, span := syncTracer.Start(ctx, "RunSyncHandler")
	defer span.End()

	tasks, err := claimBatchSyncTasks(ctx, d)
	if err != nil {
		span.RecordError(err)
		log.Printf("sync: claim batch (traceID=%s): %v", span.SpanContext().TraceID(), err)
		return false
	}

	if len(tasks) == 0 {
		return true
	}

	if onStarted != nil {
		onStarted()
	}

	for _, task := range tasks {
		task := task
		go processSyncTaskWorker(context.Background(), d, task)
	}

	return true
}

// claimBatchSyncTasks atomically claims up to syncBatchSize PENDING SyncTask rows,
// changing their status to PROCESSED. Rows are ordered hours_count DESC so those
// nearest completion are dispatched first. Returns the claimed rows with their
// current field values.
func claimBatchSyncTasks(ctx context.Context, d *dal.DAL) ([]*ent.SyncTask, error) {
	var tasks []*ent.SyncTask
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			WITH claimed AS (
				SELECT id
				FROM ingestion.sync_tasks
				WHERE status = $1
				ORDER BY hours_count DESC, created_at ASC
				FOR UPDATE SKIP LOCKED
				LIMIT $2
			)
			UPDATE ingestion.sync_tasks AS st
			SET status = $3
			FROM claimed
			WHERE st.id = claimed.id
			RETURNING st.id, st.instrument_id, st.target_date, st.status, st.hours_count, st.created_at
		`,
			string(synctask.StatusPENDING),
			syncBatchSize,
			string(synctask.StatusPROCESSED),
		)
		if err != nil {
			return err
		}
		defer func(rows *sql.Rows) { _ = rows.Close() }(rows)

		for rows.Next() {
			task := &ent.SyncTask{}
			var status string
			if err := rows.Scan(
				&task.ID, &task.InstrumentID, &task.TargetDate,
				&status, &task.HoursCount, &task.CreatedAt,
			); err != nil {
				return err
			}
			task.Status = synctask.Status(status)
			tasks = append(tasks, task)
		}
		return rows.Err()
	})
	return tasks, err
}

// processSyncTaskWorker is the per-task goroutine body.
//
// Step 1 (no lock): COUNT CONFIRMED TICK rows for the task's 24-hour window.
//   - If count < 24: update hours_count and reset the task to PENDING so the
//     next trigger re-evaluates it (prioritised by hours_count DESC).
//   - If count == 24: proceed to Step 2.
//
// Step 2 (LockIDSync held): acquire the advisory lock, then in ONE pool
// transaction update the CANDLE's resolved_tick_count and hard-delete the
// SyncTask row. The advisory lock serialises concurrent goroutines that
// simultaneously reach count == 24.
func processSyncTaskWorker(ctx context.Context, d *dal.DAL, task *ent.SyncTask) {
	defer recoverGoroutine(ctx, "processSyncTaskWorker")

	_, span := syncTracer.Start(ctx, "sync/process-task",
		trace.WithAttributes(attribute.String("task.id", task.ID.String())))
	defer span.End()

	windowStart := task.TargetDate
	windowEnd := task.TargetDate.Add(24 * time.Hour)

	// Step 1: count + branch.
	var actualCount int
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		var err error
		actualCount, err = tx.State.Query().
			Where(
				state.InstrumentID(task.InstrumentID),
				state.JobTypeEQ(state.JobTypeTICK),
				state.StatusEQ(state.StatusCONFIRMED),
				state.TimestampGTE(windowStart),
				state.TimestampLT(windowEnd),
			).
			Count(ctx)
		if err != nil {
			return fmt.Errorf("sync count ticks instrument=%s date=%s: %w",
				task.InstrumentID, task.TargetDate.Format(time.DateOnly), err)
		}

		if actualCount < 24 {
			// Update hours_count and reset to PENDING for next cycle.
			rows, qErr := tx.QueryContext(ctx,
				`UPDATE ingestion.sync_tasks
				 SET hours_count = $1, status = 'PENDING'
				 WHERE id = $2
				 RETURNING id`,
				actualCount, task.ID,
			)
			if qErr != nil {
				return fmt.Errorf("sync reset task %s: %w", task.ID, qErr)
			}
			rows.Close()
		}
		return nil
	}); err != nil {
		span.RecordError(err)
		log.Printf("sync: task %s count/reset (traceID=%s): %v",
			task.ID, span.SpanContext().TraceID(), err)
		return
	}

	if actualCount < 24 {
		log.Printf("sync: task %s instrument=%s date=%s hours_count=%d — reset to PENDING",
			task.ID, task.InstrumentID, task.TargetDate.Format(time.DateOnly), actualCount)
		return
	}

	// Step 2: all 24 hours confirmed — acquire LockIDSync then finalise the candle.
	lockTx, err := d.WaitAndAcquireAdvisoryLock(ctx, dal.LockIDSync)
	if err != nil {
		span.RecordError(err)
		log.Printf("sync: task %s acquire lock (traceID=%s): %v",
			task.ID, span.SpanContext().TraceID(), err)
		return
	}
	defer func() { _ = lockTx.Rollback() }()

	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		if err := tx.State.Update().
			Where(
				state.InstrumentID(task.InstrumentID),
				state.JobTypeEQ(state.JobTypeCANDLE),
				state.TimestampEQ(windowStart),
			).
			SetResolvedTickCount(actualCount).
			SetUpdatedAt(time.Now().UTC()).
			Exec(ctx); err != nil {
			return fmt.Errorf("sync update candle instrument=%s date=%s: %w",
				task.InstrumentID, task.TargetDate.Format(time.DateOnly), err)
		}

		// Conditional delete — only removes the row while it is still PROCESSED.
		// If a concurrent TICK event has already reset it to PENDING, this predicate
		// matches zero rows and the task survives for the next cycle.
		if _, err := tx.SyncTask.Delete().
			Where(
				synctask.IDEQ(task.ID),
				synctask.StatusEQ(synctask.StatusPROCESSED),
			).Exec(ctx); err != nil {
			return fmt.Errorf("sync delete task %s: %w", task.ID, err)
		}
		return nil
	}); err != nil {
		span.RecordError(err)
		log.Printf("sync: task %s finalize (traceID=%s): %v",
			task.ID, span.SpanContext().TraceID(), err)
		return
	}

	log.Printf("sync: task %s complete — candle instrument=%s date=%s resolved (count=%d)",
		task.ID, task.InstrumentID, task.TargetDate.Format(time.DateOnly), actualCount)
}
