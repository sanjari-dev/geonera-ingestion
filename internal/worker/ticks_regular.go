package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	"time"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/instrument"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	ilogger "github.com/sanjari-dev/geonera-ingestion/internal/logger"
)

// ── T-0: current hour ingestion ───────────────────────────────────────────────

func runT0Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "ticks/T0")

	target := time.Now().UTC().Truncate(time.Hour)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runT0Phase", "target": target.Format(time.RFC3339)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runT0Phase").Trace("fn_exit")

	logrus.WithField("target", target.Format(time.RFC3339)).Info("ticks/T0: start")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runT0Phase", "target": target.Format(time.RFC3339)}).Trace("before call: ensureT0TickTasks")
	ensureT0TickTasks(ctx, d, target)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runT0Phase"}).Trace("after call: ensureT0TickTasks")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runT0Phase", "target": target.Format(time.RFC3339)}).Trace("before call: runIngestionLoop")
	runIngestionLoop(ctx, d, &target)
	ilogger.T(ctx).WithField("fn", "runT0Phase").Trace("after call: runIngestionLoop")

	logrus.WithField("target", target.Format(time.RFC3339)).Info("ticks/T0: done")
}

// ensureT0TickTasks inserts a PENDING TICK row at target for every instrument
// where IsActive=true AND IsPause=false that does not already have one.
// Each instrument gets exactly one task per hour; duplicates are silently
// ignored via ON CONFLICT (instrument_id, timestamp, job_type) DO NOTHING.
func ensureT0TickTasks(ctx context.Context, d *dal.DAL, target time.Time) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "ensureT0TickTasks", "target": target.Format(time.RFC3339)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "ensureT0TickTasks").Trace("fn_exit")

	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.Query.All"}).Trace("before query")
		instruments, err := tx.Instrument.Query().
			Where(
				instrument.IsActiveEQ(true),
				instrument.IsPauseEQ(false),
			).
			All(ctx)
		if err != nil {
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.Query.All", "err": true}).Trace("after query")
			return fmt.Errorf("query instruments: %w", err)
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.Query.All", "count": len(instruments)}).Trace("after query")
		logrus.WithFields(logrus.Fields{"count": len(instruments), "target": target.Format(time.RFC3339)}).Info("ticks/T0: seeding PENDING tasks")
		insertedAt := time.Now().UTC()
		for _, inst := range instruments {
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "instrument": inst.Name}).Trace("before query")
			if err := insertStatePendingOnConflictDoNothing(ctx, tx, inst.ID, state.JobTypeTICK, target, insertedAt); err != nil {
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "err": true}).Trace("after query")
				return fmt.Errorf("ensure T0 task %s at %s: %w", inst.Name, target, err)
			}
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "instrument": inst.Name, "err": false}).Trace("after query")
		}
		return nil
	}); err != nil {
		logrus.WithError(err).WithField("target", target.Format(time.RFC3339)).Error("ticks/T0: ensure tasks error")
	}
}

// ── T-1: previous hour recovery ───────────────────────────────────────────────

func runT1Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "ticks/T1")

	target := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runT1Phase", "target": target.Format(time.RFC3339)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runT1Phase").Trace("fn_exit")

	logrus.WithField("target", target.Format(time.RFC3339)).Info("ticks/T1: start")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runT1Phase", "target": target.Format(time.RFC3339)}).Trace("before call: runRecoveryLoop")
	runRecoveryLoop(ctx, d, target)
	ilogger.T(ctx).WithField("fn", "runT1Phase").Trace("after call: runRecoveryLoop")

	logrus.WithField("target", target.Format(time.RFC3339)).Info("ticks/T1: done")
}

// ── T-2: two-hours-ago physical validation ────────────────────────────────────

func runT2Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "ticks/T2")

	target := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runT2Phase", "target": target.Format(time.RFC3339)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runT2Phase").Trace("fn_exit")

	logrus.WithField("target", target.Format(time.RFC3339)).Info("ticks/T2: start")
	// T-2 Regular: claims COMPLETED + NOT_FOUND, re-downloads BI5 for cross-validation.
	// COMPLETED → re-download matches → CONFIRMED; NOT_FOUND → streak+1, at ≥3 → CONFIRMED.
	// IsPause=false enforced, consistent with T-0 and T-1.
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runT2Phase", "target": target.Format(time.RFC3339)}).Trace("before call: runValidationLoop")
	runValidationLoop(ctx, d, &target)
	ilogger.T(ctx).WithField("fn", "runT2Phase").Trace("after call: runValidationLoop")

	logrus.WithField("target", target.Format(time.RFC3339)).Info("ticks/T2: done")
}

// runIngestionLoop claims PENDING TICK rows one at a time for the given hour
// and processes each through the ETL pipeline in its own goroutine.
//
// Used by T-0 (current-hour ingestion) with a non-nil timestamp — at most one
// row per active instrument, so the natural fan-out is small. Backfill no
// longer drives this loop; it is PENDING rows are claimed and dispatched as part
// of the Master Bulk-Claim (see runBackfillMasterClaimLoop / claimBackfillMasterBatch).
//
// Concurrency is bounded by the real bottlenecks deeper in the pipeline —
// the download gate (12 concurrent workers, download_gate.go) and
// tickProcessSem (runtime.NumCPU() concurrent convert+upload) — not by an
// artificial goroutine-pool cap here.
func runIngestionLoop(ctx context.Context, d *dal.DAL, timestamp *time.Time) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runIngestionLoop"}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runIngestionLoop").Trace("fn_exit")

	var loopWg sync.WaitGroup
	count := 0
	for ctx.Err() == nil {
		ilogger.T(ctx).WithField("fn", "runIngestionLoop").Trace("loop: iteration start")

		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked"}).Trace("before query")
		claimed, err := claimStateAsProcessed(ctx, d, func(q *ent.StateQuery) *ent.StateQuery {
			query := tickActiveStateRows(q,
				state.StatusEQ(state.StatusPENDING),
			).
				WithInstrument().
				Order(state.ByTimestamp())
			if timestamp != nil {
				query = query.Where(state.TimestampEQ(*timestamp))
			}
			return query
		})
		if err != nil {
			if ent.IsNotFound(err) {
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "found": false}).Trace("after query")
				break
			}
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "err": true}).Trace("after query")
			logrus.WithError(err).Error("ticks/T0: ingestion claim error")
			break
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "found": true, "instrument": instrName(claimed)}).Trace("after query")

		count++
		logrus.WithFields(logrus.Fields{
			"instrument": instrName(claimed),
			"ts":         claimed.Timestamp.Format(time.RFC3339),
			"state_id":   claimed.ID,
		}).Info("ticks/T0: claimed")

		loopWg.Add(1)
		go func(r *ent.State) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "ticks/ingestion-row")
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(r), "ts": r.Timestamp.Format(time.RFC3339)}).Trace("goroutine: ingestion-row start")
			executeIngestionETL(ctx, d, r, handleNotFoundSimple, true)
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(r)}).Trace("goroutine: ingestion-row done")
		}(claimed)

		ilogger.T(ctx).WithField("fn", "runIngestionLoop").Trace("loop: iteration end")
	}
	ilogger.T(ctx).WithField("fn", "runIngestionLoop").Trace("before loopWg.Wait")
	loopWg.Wait()
	ilogger.T(ctx).WithField("fn", "runIngestionLoop").Trace("after loopWg.Wait")
	if timestamp != nil {
		logrus.WithFields(logrus.Fields{"rows": count, "target": timestamp.Format(time.RFC3339)}).Info("ticks/T0: ingestion loop complete")
	}
}

// runRecoveryLoop claims PENDING/PROCESSED/FAILED/BROKEN/NOT_FOUND rows for the
// given timestamp and applies the T-1 recovery rules to each.
func runRecoveryLoop(ctx context.Context, d *dal.DAL, target time.Time) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runRecoveryLoop", "target": target.Format(time.RFC3339)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runRecoveryLoop").Trace("fn_exit")

	var loopWg sync.WaitGroup
	count := 0
	for ctx.Err() == nil {
		ilogger.T(ctx).WithField("fn", "runRecoveryLoop").Trace("loop: iteration start")

		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked"}).Trace("before query")
		claimed, err := claimStateAsProcessed(ctx, d, func(q *ent.StateQuery) *ent.StateQuery {
			return tickActiveStateRows(q,
				state.StatusIn(
					state.StatusPENDING,
					state.StatusPROCESSED,
					state.StatusFAILED,
					state.StatusBROKEN,
					state.StatusNOT_FOUND,
				),
				state.TimestampEQ(target),
			).
				WithInstrument().
				Order(
					// Process stuck PROCESSED first, then FAILED/BROKEN, then NOT_FOUND, then PENDING.
					orderStateStatuses(
						state.StatusPROCESSED,
						state.StatusFAILED,
						state.StatusBROKEN,
						state.StatusNOT_FOUND,
						state.StatusPENDING,
					),
					state.ByTimestamp(),
				)
		})
		if err != nil {
			if ent.IsNotFound(err) {
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "found": false}).Trace("after query")
				break
			}
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "err": true}).Trace("after query")
			logrus.WithError(err).WithField("target", target.Format(time.RFC3339)).Error("ticks/T1: recovery claim error")
			break
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "found": true, "instrument": instrName(claimed)}).Trace("after query")

		count++
		logrus.WithFields(logrus.Fields{
			"instrument":  instrName(claimed),
			"ts":          claimed.Timestamp.Format(time.RFC3339),
			"state_id":    claimed.ID,
			"prev_status": prevStatusStr(claimed.PreviousStatus),
		}).Info("ticks/T1: claimed")

		prevStatus := claimed.PreviousStatus
		loopWg.Add(1)
		go func(r *ent.State, prev *state.PreviousStatus) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "ticks/recovery-row")
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(r), "ts": r.Timestamp.Format(time.RFC3339)}).Trace("goroutine: recovery-row start")
			executeRecoveryAction(ctx, d, r, prev)
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(r)}).Trace("goroutine: recovery-row done")
		}(claimed, prevStatus)

		ilogger.T(ctx).WithField("fn", "runRecoveryLoop").Trace("loop: iteration end")
	}
	ilogger.T(ctx).WithField("fn", "runRecoveryLoop").Trace("before loopWg.Wait")
	loopWg.Wait()
	ilogger.T(ctx).WithField("fn", "runRecoveryLoop").Trace("after loopWg.Wait")
	logrus.WithFields(logrus.Fields{"rows": count, "target": target.Format(time.RFC3339)}).Info("ticks/T1: recovery loop complete")
}

// runValidationLoop claims COMPLETED and NOT_FOUND rows for the given hour
// and cross-validates each one via executeT2Action (re-download from Dukascopy):
//
//   - COMPLETED + 2xx → convert+upload(overwrite)+validate → CONFIRMED or BROKEN.
//   - NOT_FOUND + 2xx → SetNotFoundStreak=0, Status=PENDING (ETL next cycle).
//   - NOT_FOUND + 404 → streak+1; at notFoundThreshold(3): executeValidation → CONFIRMED.
//   - COMPLETED + 404 → BROKEN (anomaly: data was present at T-0 time).
//   - Any non-404 error → FAILED.
//
// Used by T-2 (two-hours-ago) with a non-nil timestamp.
// Backfill drives its own rows via the Master Bulk-Claim.
func runValidationLoop(ctx context.Context, d *dal.DAL, timestamp *time.Time) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runValidationLoop"}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runValidationLoop").Trace("fn_exit")

	var loopWg sync.WaitGroup
	count := 0
	for ctx.Err() == nil {
		ilogger.T(ctx).WithField("fn", "runValidationLoop").Trace("loop: iteration start")

		// preclaimPrev is the row's PreviousStatus BEFORE the claim — forwarded to
		// executeT2Action to detect genuine demotions (CONFIRMED → BROKEN).
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked"}).Trace("before query")
		claimed, preclaimPrev, err := claimStateWithPrevious(ctx, d, func(q *ent.StateQuery) *ent.StateQuery {
			query := q.
				Where(
					state.JobTypeEQ(state.JobTypeTICK),
					state.StatusIn(state.StatusCOMPLETED, state.StatusNOT_FOUND),
					state.HasInstrumentWith(instrument.IsActiveEQ(true), instrument.IsPauseEQ(false)),
					state.IsDeletedEQ(false),
				).
				WithInstrument().
				Order(state.ByTimestamp())
			if timestamp != nil {
				query = query.Where(state.TimestampEQ(*timestamp))
			}
			return query
		}, func(row *ent.State) (state.PreviousStatus, state.Status) {
			return state.PreviousStatus(row.Status), state.StatusPROCESSED
		})
		if err != nil {
			if ent.IsNotFound(err) {
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "found": false}).Trace("after query")
				break
			}
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "err": true}).Trace("after query")
			logrus.WithError(err).Error("ticks/T2: validation claim error")
			break
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "found": true, "instrument": instrName(claimed)}).Trace("after query")

		count++
		logrus.WithFields(logrus.Fields{
			"instrument":  instrName(claimed),
			"ts":          claimed.Timestamp.Format(time.RFC3339),
			"state_id":    claimed.ID,
			"prev_status": prevStatusStr(preclaimPrev),
		}).Info("ticks/T2: claimed")

		loopWg.Add(1)
		go func(r *ent.State, prev *state.PreviousStatus) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "ticks/validation-row")
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(r), "ts": r.Timestamp.Format(time.RFC3339)}).Trace("goroutine: validation-row start")
			executeT2Action(ctx, d, r, prev, true)
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(r)}).Trace("goroutine: validation-row done")
		}(claimed, preclaimPrev)

		ilogger.T(ctx).WithField("fn", "runValidationLoop").Trace("loop: iteration end")
	}
	ilogger.T(ctx).WithField("fn", "runValidationLoop").Trace("before loopWg.Wait")
	loopWg.Wait()
	ilogger.T(ctx).WithField("fn", "runValidationLoop").Trace("after loopWg.Wait")
	if timestamp != nil {
		logrus.WithFields(logrus.Fields{"rows": count, "target": timestamp.Format(time.RFC3339)}).Info("ticks/T2: validation loop complete")
	}
}

// executeRecoveryAction applies the T-1 recovery rule for a claimed row based
// on the status it held before being claimed.
func executeRecoveryAction(ctx context.Context, d *dal.DAL, row *ent.State, prev *state.PreviousStatus) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeRecoveryAction", "instrument": instrName(row), "prev": prevStatusStr(prev)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "executeRecoveryAction").Trace("fn_exit")

	if prev == nil {
		ilogger.T(ctx).WithField("fn", "executeRecoveryAction").Trace("branch: prev_nil_return")
		return
	}
	inst := instrName(row)
	ts := row.Timestamp.Format(time.RFC3339)
	switch *prev {
	case state.PreviousStatusPROCESSED, state.PreviousStatusPENDING:
		// Stuck PROCESSED or stale PENDING → reset to PENDING for the next T-0 run.
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "prev": *prev}).Trace("branch: T1_zombie_reset_pending")
		logrus.WithFields(logrus.Fields{"instrument": inst, "ts": ts, "prev_status": *prev}).Info("ticks/T1: zombie → PENDING")
		ilogger.T(ctx).WithField("fn", "executeRecoveryAction").Trace("before call: resetStateToPending")
		resetStateToPending(ctx, d, row)
		ilogger.T(ctx).WithField("fn", "executeRecoveryAction").Trace("after call: resetStateToPending")

	case state.PreviousStatusFAILED, state.PreviousStatusBROKEN:
		// Increment RetryCount, decrement NotFoundStreak (floor 0), always → PENDING.
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "prev": *prev, "retry_count": row.RetryCount}).Trace("branch: T1_retry_reset")
		logrus.WithFields(logrus.Fields{"instrument": inst, "ts": ts, "prev_status": *prev, "retry_count": row.RetryCount}).Info("ticks/T1: retry → PENDING")
		ilogger.T(ctx).WithField("fn", "executeRecoveryAction").Trace("before call: handleRetryReset")
		handleRetryReset(ctx, d, row)
		ilogger.T(ctx).WithField("fn", "executeRecoveryAction").Trace("after call: handleRetryReset")

	case state.PreviousStatusNOT_FOUND:
		// Re-check API: data → PENDING+streak=0; still 404 → Zero-Row+NOT_FOUND.
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "streak": row.NotFoundStreak}).Trace("branch: T1_not_found_recheck")
		logrus.WithFields(logrus.Fields{"instrument": inst, "ts": ts, "streak": row.NotFoundStreak}).Info("ticks/T1: recheck")
		ilogger.T(ctx).WithField("fn", "executeRecoveryAction").Trace("before call: executeNotFoundRecheck")
		executeNotFoundRecheck(ctx, d, row, true)
		ilogger.T(ctx).WithField("fn", "executeRecoveryAction").Trace("after call: executeNotFoundRecheck")
	}
}
