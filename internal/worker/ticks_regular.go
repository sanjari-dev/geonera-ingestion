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
)

// ── T-0: current hour ingestion ───────────────────────────────────────────────

func runT0Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "ticks/T0")

	target := time.Now().UTC().Truncate(time.Hour)
	logrus.Infof("ticks/T0: start target=%s", target.Format(time.RFC3339))

	ensureT0TickTasks(ctx, d, target)
	runIngestionLoop(ctx, d, &target)

	logrus.Infof("ticks/T0: done target=%s", target.Format(time.RFC3339))
}

// ensureT0TickTasks inserts a PENDING TICK row at target for every instrument
// where IsActive=true AND IsPause=false that does not already have one.
// Each instrument gets exactly one task per hour; duplicates are silently
// ignored via ON CONFLICT (instrument_id, timestamp, job_type) DO NOTHING.
func ensureT0TickTasks(ctx context.Context, d *dal.DAL, target time.Time) {
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		instruments, err := tx.Instrument.Query().
			Where(
				instrument.IsActiveEQ(true),
				instrument.IsPauseEQ(false),
			).
			All(ctx)
		if err != nil {
			return fmt.Errorf("query instruments: %w", err)
		}
		logrus.Infof("ticks/T0: seeding PENDING task(s) for %d instrument(s) at %s", len(instruments), target.Format(time.RFC3339))
		insertedAt := time.Now().UTC()
		for _, inst := range instruments {
			if err := insertStatePendingOnConflictDoNothing(ctx, tx, inst.ID, state.JobTypeTICK, target, insertedAt); err != nil {
				return fmt.Errorf("ensure T0 task %s at %s: %w", inst.Name, target, err)
			}
		}
		return nil
	}); err != nil {
		logrus.WithError(err).Errorf("ticks/T0: ensure tasks error target=%s", target.Format(time.RFC3339))
	}
}

// ── T-1: previous hour recovery ───────────────────────────────────────────────

func runT1Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "ticks/T1")

	target := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	logrus.Infof("ticks/T1: start target=%s", target.Format(time.RFC3339))
	runRecoveryLoop(ctx, d, target)
	logrus.Infof("ticks/T1: done target=%s", target.Format(time.RFC3339))
}

// ── T-2: two-hours-ago physical validation ────────────────────────────────────

func runT2Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "ticks/T2")

	target := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	logrus.Infof("ticks/T2: start target=%s", target.Format(time.RFC3339))
	// T-2 Regular: claims COMPLETED + NOT_FOUND, re-downloads BI5 for cross-validation.
	// COMPLETED → re-download matches → CONFIRMED; NOT_FOUND → streak+1, at ≥3 → CONFIRMED.
	// IsPause=false enforced, consistent with T-0 and T-1.
	runValidationLoop(ctx, d, &target)
	logrus.Infof("ticks/T2: done target=%s", target.Format(time.RFC3339))
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
	var loopWg sync.WaitGroup
	count := 0
	for ctx.Err() == nil {
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
				break
			}
			logrus.WithError(err).Error("ticks/T0: ingestion claim error")
			break
		}

		count++
		logrus.Infof("ticks/T0: claimed instrument=%s ts=%s state_id=%s",
			instrName(claimed), claimed.Timestamp.Format(time.RFC3339), claimed.ID)

		loopWg.Add(1)
		go func(r *ent.State) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "ticks/ingestion-row")
			executeIngestionETL(ctx, d, r, handleNotFoundSimple, true)
		}(claimed)
	}
	loopWg.Wait()
	if timestamp != nil {
		logrus.Infof("ticks/T0: ingestion loop complete rows=%d target=%s", count, timestamp.Format(time.RFC3339))
	}
}

// runRecoveryLoop claims PENDING/PROCESSED/FAILED/BROKEN/NOT_FOUND rows for the
// given timestamp and applies the T-1 recovery rules to each.
func runRecoveryLoop(ctx context.Context, d *dal.DAL, target time.Time) {
	var loopWg sync.WaitGroup
	count := 0
	for ctx.Err() == nil {
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
				break
			}
			logrus.WithError(err).Errorf("ticks/T1: recovery claim error target=%s", target.Format(time.RFC3339))
			break
		}

		count++
		logrus.Infof("ticks/T1: claimed instrument=%s ts=%s state_id=%s prev_status=%s",
			instrName(claimed), claimed.Timestamp.Format(time.RFC3339), claimed.ID, prevStatusStr(claimed.PreviousStatus))

		prevStatus := claimed.PreviousStatus
		loopWg.Add(1)
		go func(r *ent.State, prev *state.PreviousStatus) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "ticks/recovery-row")
			executeRecoveryAction(ctx, d, r, prev)
		}(claimed, prevStatus)
	}
	loopWg.Wait()
	logrus.Infof("ticks/T1: recovery loop complete rows=%d target=%s", count, target.Format(time.RFC3339))
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
	var loopWg sync.WaitGroup
	count := 0
	for ctx.Err() == nil {
		// preclaimPrev is the row's PreviousStatus BEFORE the claim — forwarded to
		// executeT2Action to detect genuine demotions (CONFIRMED → BROKEN).
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
				break
			}
			logrus.WithError(err).Error("ticks/T2: validation claim error")
			break
		}

		count++
		logrus.Infof("ticks/T2: claimed instrument=%s ts=%s state_id=%s preclaim_prev=%s",
			instrName(claimed), claimed.Timestamp.Format(time.RFC3339), claimed.ID, prevStatusStr(preclaimPrev))

		loopWg.Add(1)
		go func(r *ent.State, prev *state.PreviousStatus) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "ticks/validation-row")
			executeT2Action(ctx, d, r, prev, true)
		}(claimed, preclaimPrev)
	}
	loopWg.Wait()
	if timestamp != nil {
		logrus.Infof("ticks/T2: validation loop complete rows=%d target=%s", count, timestamp.Format(time.RFC3339))
	}
}

// executeRecoveryAction applies the T-1 recovery rule for a claimed row based
// on the status it held before being claimed.
func executeRecoveryAction(ctx context.Context, d *dal.DAL, row *ent.State, prev *state.PreviousStatus) {
	if prev == nil {
		return
	}
	inst := instrName(row)
	ts := row.Timestamp.Format(time.RFC3339)
	switch *prev {
	case state.PreviousStatusPROCESSED, state.PreviousStatusPENDING:
		// Stuck PROCESSED or stale PENDING → reset to PENDING for the next T-0 run.
		logrus.Infof("ticks/T1: zombie instrument=%s ts=%s prev_status=%s → PENDING", inst, ts, *prev)
		resetStateToPending(ctx, d, row)

	case state.PreviousStatusFAILED, state.PreviousStatusBROKEN:
		// Increment RetryCount, decrement NotFoundStreak (floor 0), always → PENDING.
		logrus.Infof("ticks/T1: retry instrument=%s ts=%s prev_status=%s retry_count=%d → PENDING", inst, ts, *prev, row.RetryCount)
		handleRetryReset(ctx, d, row)

	case state.PreviousStatusNOT_FOUND:
		// Re-check API: data → PENDING+streak=0; still 404 → Zero-Row+NOT_FOUND.
		logrus.Infof("ticks/T1: recheck instrument=%s ts=%s streak=%d", inst, ts, row.NotFoundStreak)
		executeNotFoundRecheck(ctx, d, row, true)
	}
}
