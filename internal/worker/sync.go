package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"entgo.io/ent/dialect/sql"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/ent/synctask"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	ilogger "github.com/sanjari-dev/geonera-ingestion/internal/logger"
)

// syncBatchSize is the maximum number of SyncTask rows claimed per trigger.
const syncBatchSize = 12

// syncWorkerTimeout is the maximum lifetime of a per-task sync goroutine.
const syncWorkerTimeout = 5 * time.Minute

// RunSyncHandler claims up to syncBatchSize PENDING SyncTask rows ordered by
// hours_count DESC (closest to 24 processed first) and dispatches one goroutine
// per row as fire-and-forget. The handler returns immediately after launching goroutines.
func RunSyncHandler(ctx context.Context, d *dal.DAL, onStarted func()) bool {
	ilogger.T(ctx).WithField("fn", "RunSyncHandler").Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "RunSyncHandler").Trace("fn_exit")

	ilogger.T(ctx).WithField("fn", "RunSyncHandler").Trace("before call: claimBatchSyncTasks")
	tasks, err := claimBatchSyncTasks(ctx, d)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "RunSyncHandler", "tasks": len(tasks), "err": err != nil}).Trace("after call: claimBatchSyncTasks")
	if err != nil {
		ilogger.T(ctx).WithError(err).Error("sync: claim batch")
		return false
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"tasks_claimed": len(tasks), "batch_size": syncBatchSize}).Debug("sync: claim vars")

	if len(tasks) == 0 {
		ilogger.T(ctx).WithField("fn", "RunSyncHandler").Trace("branch: no tasks claimed")
		return true
	}

	if onStarted != nil {
		onStarted()
	}

	for _, task := range tasks {
		task := task
		workerCtx, cancel := context.WithTimeout(ctx, syncWorkerTimeout)
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "RunSyncHandler", "task_id": task.ID}).Trace("goroutine: sync worker dispatch")
		go func() {
			defer cancel()
			ilogger.T(workerCtx).WithFields(logrus.Fields{"task_id": task.ID, "instrument": task.InstrumentID}).Trace("goroutine: sync worker start")
			processSyncTaskWorker(workerCtx, d, task)
			ilogger.T(workerCtx).WithFields(logrus.Fields{"task_id": task.ID}).Trace("goroutine: sync worker done")
		}()
	}

	return true
}

// claimBatchSyncTasks atomically claims up to syncBatchSize PENDING SyncTask rows,
// changing their status to PROCESSED. Rows are ordered hours_count DESC so those
// nearest completion are dispatched first. Returns the claimed rows with their
// current field values.
func claimBatchSyncTasks(ctx context.Context, d *dal.DAL) ([]*ent.SyncTask, error) {
	ilogger.T(ctx).WithField("fn", "claimBatchSyncTasks").Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "claimBatchSyncTasks").Trace("fn_exit")

	var tasks []*ent.SyncTask
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "fn": "claimBatchSyncTasks"}).Trace("before query")
	err := d.Execute(ctx, func(tx *ent.Tx) error {
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
			ilogger.T(ctx).WithField("fn", "claimBatchSyncTasks").Trace("loop: scan sync task row")
			task := &ent.SyncTask{}
			var status string
			if err := rows.Scan(
				&task.ID, &task.InstrumentID, &task.TargetDate,
				&status, &task.HoursCount, &task.CreatedAt,
			); err != nil {
				return err
			}
			task.Status = synctask.Status(status)
			ilogger.T(ctx).WithFields(logrus.Fields{"task_id": task.ID, "instrument": task.InstrumentID, "date": task.TargetDate.Format(time.DateOnly), "hours_count": task.HoursCount, "status": task.Status}).Debug("sync: scanned task row vars")
			tasks = append(tasks, task)
		}
		return rows.Err()
	})
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "count": len(tasks), "err": err != nil}).Trace("after query")
	return tasks, err
}

// processSyncTaskWorker is the per-task goroutine body.
//
// Step 1 (no lock): COUNT CONFIRMED TICK rows for the task's 24-hour window.
//   - If count < 24: update hours_count and reset the task to PENDING so the
//     next trigger re-evaluates it (prioritized by hours_count DESC).
//   - If count == 24: proceed to Step 2.
//
// Step 2: all 24 hours confirmed — update the CANDLE's resolved_tick_count
// and hard-delete the SyncTask row in one pool transaction.
func processSyncTaskWorker(ctx context.Context, d *dal.DAL, task *ent.SyncTask) {
	defer recoverGoroutine(ctx, "processSyncTaskWorker")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "processSyncTaskWorker", "task_id": task.ID, "instrument": task.InstrumentID, "date": task.TargetDate.Format(time.DateOnly)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "processSyncTaskWorker").Trace("fn_exit")

	windowStart := task.TargetDate
	windowEnd := task.TargetDate.Add(24 * time.Hour)
	ilogger.T(ctx).WithFields(logrus.Fields{
		"task_id": task.ID, "instrument": task.InstrumentID,
		"window_start": windowStart.Format(time.DateOnly), "window_end": windowEnd.Format(time.DateOnly),
		"hours_count_before": task.HoursCount,
	}).Debug("sync: task vars")

	// Step 1: count + branch.
	var actualCount int
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.Count", "task_id": task.ID}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
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

		ilogger.T(ctx).WithFields(logrus.Fields{"actual_count": actualCount, "threshold": 24}).Trace("branch: sync_count_check")
		if actualCount < 24 {
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "actual_count": actualCount}).Trace("before query")
			// Update hours_count and reset to PENDING for next cycle.
			rows, qErr := tx.QueryContext(ctx,
				`UPDATE ingestion.sync_tasks
				 SET hours_count = $1, status = 'PENDING'
				 WHERE id = $2
				 RETURNING id`,
				actualCount, task.ID,
			)
			if qErr != nil {
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "err": true}).Trace("after query")
				return fmt.Errorf("sync reset task %s: %w", task.ID, qErr)
			}
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "err": false}).Trace("after query")
			_ = rows.Close()
		}
		return nil
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.Count", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).WithField("state_id", task.ID).Error("sync: task count/reset")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.Count", "actual_count": actualCount}).Trace("after query")
	ilogger.T(ctx).WithFields(logrus.Fields{
		"task_id": task.ID, "actual_count": actualCount, "threshold": 24, "is_ready": actualCount >= 24,
	}).Debug("sync: count vars")

	if actualCount < 24 {
		ilogger.T(ctx).WithFields(logrus.Fields{"task_id": task.ID, "actual_count": actualCount}).Trace("branch: sync_task_pending_reset")
		ilogger.T(ctx).WithFields(logrus.Fields{
			"state_id":   task.ID,
			"instrument": task.InstrumentID,
			"date":       task.TargetDate.Format(time.DateOnly),
			"rows":       actualCount,
		}).Info("sync: task reset to PENDING")
		return
	}

	ilogger.T(ctx).WithFields(logrus.Fields{"task_id": task.ID, "actual_count": actualCount}).Trace("branch: sync_task_finalize")
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Update.Exec + SyncTask.Delete.Exec", "task_id": task.ID}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
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

		// Conditionally delete it — only removes the row while it is still PROCESSED.
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
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Update.Exec + SyncTask.Delete.Exec", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).WithField("state_id", task.ID).Error("sync: task finalize")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Update.Exec + SyncTask.Delete.Exec", "err": false}).Trace("after query")

	ilogger.T(ctx).WithFields(logrus.Fields{
		"state_id":   task.ID,
		"instrument": task.InstrumentID,
		"date":       task.TargetDate.Format(time.DateOnly),
		"rows":       actualCount,
	}).Info("sync: task complete — candle resolved")
}
