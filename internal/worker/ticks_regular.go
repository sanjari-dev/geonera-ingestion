package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/instrument"
	"github.com/sanjari-dev/geonera-ingestion/ent/predicate"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
)

// ── T-0: current hour ingestion ───────────────────────────────────────────────

func runT0Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "ticks/T0")

	target := time.Now().UTC().Truncate(time.Hour)

	// Guarantee every eligible instrument has a PENDING row for this hour before
	// claiming. In the normal flow maintenance already seeded these rows; this
	// call is a safety net for instruments that were just activated or whose row
	// was not yet seeded by the time T-0 fires. ON CONFLICT DO NOTHING makes it
	// a no-op when the row already exists.
	ensureT0TickTasks(ctx, d, target)

	runIngestionLoop(ctx, d, &target)
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
		insertedAt := time.Now().UTC()
		for _, inst := range instruments {
			if err := insertStatePendingOnConflictDoNothing(ctx, tx, inst.ID, state.JobTypeTICK, target, insertedAt); err != nil {
				return fmt.Errorf("ensure T0 task %s at %s: %w", inst.Name, target, err)
			}
		}
		return nil
	}); err != nil {
		log.Printf("T0 ensure tasks: %v", err)
	}
}

// ── T-1: previous hour recovery ───────────────────────────────────────────────

func runT1Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "ticks/T1")

	target := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	runRecoveryLoop(ctx, d, target)
}

// ── T-2: two-hours-ago physical validation ────────────────────────────────────

func runT2Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "ticks/T2")

	target := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	// T-2 Regular: claims COMPLETED + NOT_FOUND, re-downloads BI5 for cross-validation.
	// COMPLETED → re-download matches → CONFIRMED; NOT_FOUND → streak+1, at ≥3 → CONFIRMED.
	// Only IsActive filter — paused instruments still get validated.
	runValidationLoop(ctx, d, &target, false)
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
	for ctx.Err() == nil {
		claimed, err := claimStateAsProcessed(ctx, d, func(q *ent.StateQuery) *ent.StateQuery {
			query := tickActiveStateRows(q,
				state.StatusEQ(state.StatusPENDING),
			).
				WithInstrument(). // needed by downloadBI5 / convertBI5ToParquet / uploadToR2
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
			log.Printf("ingestion claim: %v", err)
			break
		}

		loopWg.Add(1)
		go func(r *ent.State) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "ticks/ingestion-row")
			executeIngestionETL(ctx, d, r, handleNotFoundSimple)
		}(claimed)
	}
	loopWg.Wait()
}

// runRecoveryLoop claims PENDING/PROCESSED/FAILED/BROKEN/NOT_FOUND rows for the
// given timestamp and applies the T-1 recovery rules to each.
func runRecoveryLoop(ctx context.Context, d *dal.DAL, target time.Time) {
	var loopWg sync.WaitGroup
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
			log.Printf("recovery claim (target %s): %v", target, err)
			break
		}

		prevStatus := claimed.PreviousStatus
		loopWg.Add(1)
		go func(r *ent.State, prev *state.PreviousStatus) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "ticks/recovery-row")
			executeRecoveryAction(ctx, d, r, prev)
		}(claimed, prevStatus)
	}
	loopWg.Wait()
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
// respectPause=false for T-2 Regular: paused instruments still get validated.
func runValidationLoop(ctx context.Context, d *dal.DAL, timestamp *time.Time, respectPause bool) {
	var loopWg sync.WaitGroup
	for ctx.Err() == nil {
		// preclaimPrev is the row's PreviousStatus BEFORE the claim — forwarded to
		// executeT2Action to detect genuine demotions (CONFIRMED → BROKEN).
		claimed, preclaimPrev, err := claimStateWithPrevious(ctx, d, func(q *ent.StateQuery) *ent.StateQuery {
			instrumentFilters := []predicate.Instrument{instrument.IsActiveEQ(true)}
			if respectPause {
				instrumentFilters = append(instrumentFilters, instrument.IsPauseEQ(false))
			}
			query := q.
				Where(
					state.JobTypeEQ(state.JobTypeTICK),
					state.StatusIn(state.StatusCOMPLETED, state.StatusNOT_FOUND),
					state.HasInstrumentWith(instrumentFilters...),
					state.IsDeletedEQ(false),
				).
				WithInstrument(). // needed for BI5 download / Parquet convert / R2 upload
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
			log.Printf("validation claim: %v", err)
			break
		}

		loopWg.Add(1)
		go func(r *ent.State, prev *state.PreviousStatus) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "ticks/validation-row")
			executeT2Action(ctx, d, r, prev)
		}(claimed, preclaimPrev)
	}
	loopWg.Wait()
}

// executeRecoveryAction applies the T-1 recovery rule for a claimed row based
// on the status it held before being claimed.
func executeRecoveryAction(ctx context.Context, d *dal.DAL, row *ent.State, prev *state.PreviousStatus) {
	if prev == nil {
		return
	}
	switch *prev {
	case state.PreviousStatusPROCESSED, state.PreviousStatusPENDING:
		// Stuck PROCESSED or stale PENDING → reset to PENDING for the next T-0 run.
		resetStateToPending(ctx, d, row)

	case state.PreviousStatusFAILED, state.PreviousStatusBROKEN:
		// Increment RetryCount, decrement NotFoundStreak (floor 0), always → PENDING.
		handleRetryReset(ctx, d, row)

	case state.PreviousStatusNOT_FOUND:
		// Re-check API: data → PENDING+streak=0; still 404 → Zero-Row+NOT_FOUND.
		executeNotFoundRecheck(ctx, d, row)
	}
}
