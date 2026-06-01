package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"entgo.io/ent/dialect/sql"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/instrument"
	"github.com/sanjari-dev/geonera-ingestion/ent/predicate"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/ent/synctask"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/dukascopy"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
	"github.com/sanjari-dev/geonera-ingestion/internal/tickparquet"
)

var tickTracer = otel.Tracer("worker/ticks")

const (
	// notFoundThreshold is the consecutive-404 count that triggers a Zero-Row Parquet commit.
	notFoundThreshold = 3
	// maxRetryCount is the ceiling for RetryCount before a row is ABANDONED.
	maxRetryCount = 5
)

// tickDownloadSem limits Dukascopy BI5 HTTP downloads (and API re-checks)
// to 12 goroutines to prevent HTTP 429 rate-limit errors.
// Shared across all tick phases (T-0, T-1, T-2, Backfill).
var tickDownloadSem = make(chan struct{}, 12)

// tickProcessSem limits concurrent Parquet convert+upload goroutines to 50
// to prevent OOM spikes on high-volume instruments like EURUSD.
// Shared across all tick phases.
var tickProcessSem = make(chan struct{}, 50)

// RunTickParentHandler is the single entry point for all tick-ingestion work.
// It acquires LockIDTick via the direct connection so that Regular and Backfill
// modes can never run concurrently, then forks the appropriate child goroutines.
//
// Mode must be either "REGULAR" or "BACKFILL".
func RunTickParentHandler(ctx context.Context, mode string, d *dal.DAL) {
	ctx, span := tickTracer.Start(ctx, fmt.Sprintf("RunTickParentHandler_%s", mode))
	defer span.End()

	tx, err := d.AcquireAdvisoryLock(ctx)
	if err != nil {
		span.RecordError(err)
		log.Printf("ticks %s: acquire lock: %v", mode, err)
		return
	}
	defer func() { _ = tx.Rollback() }()

	locked, err := tx.QueryBoolContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", dal.LockIDTick)
	if err != nil || !locked {
		span.AddEvent("LockIDTick already held by another instance, skipping")
		return
	}

	// Lock health monitor: cancel all child work if the direct connection drops.
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
				_, err := tx.QueryBoolContext(hbCtx, "SELECT true")
				hbCancel()
				if err != nil {
					span.RecordError(fmt.Errorf("lock heartbeat lost: %w", err))
					cancelETL()
					return
				}
			}
		}
	}()

	var wg sync.WaitGroup
	switch mode {
	case "REGULAR":
		wg.Add(3)
		go runT0Phase(lockCtx, d, &wg)
		go runT1Phase(lockCtx, d, &wg)
		go runT2Phase(lockCtx, d, &wg)
	case "BACKFILL":
		wg.Add(3)
		go runBackfillIngestionGroup(lockCtx, d, &wg)
		go runBackfillResetGroup(lockCtx, d, &wg)
		go runBackfillValidationGroup(lockCtx, d, &wg)
	default:
		log.Printf("ticks: unknown mode %q", mode)
	}
	wg.Wait()
}

// ── T-0: current hour ingestion ───────────────────────────────────────────────

func runT0Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "ticks/T0")

	ctx, span := tickTracer.Start(ctx, "ticks/T0")
	defer span.End()

	target := time.Now().UTC().Truncate(time.Hour)
	runIngestionLoop(ctx, d, span, &target)
}

// ── T-1: previous hour recovery ───────────────────────────────────────────────

func runT1Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "ticks/T1")

	ctx, span := tickTracer.Start(ctx, "ticks/T1")
	defer span.End()

	target := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	runRecoveryLoop(ctx, d, span, target)
}

// ── T-2: two-hours-ago physical validation ────────────────────────────────────

func runT2Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "ticks/T2")

	ctx, span := tickTracer.Start(ctx, "ticks/T2")
	defer span.End()

	target := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	// T-2 Regular: only the IsActive filter — paused instruments still get validated.
	runValidationLoop(ctx, d, span, &target, false)
}

// ── Backfill groups ───────────────────────────────────────────────────────────

func runBackfillIngestionGroup(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runBackfillIngestionGroup")

	ctx, span := tickTracer.Start(ctx, "runBackfillIngestionGroup")
	defer span.End()

	// A nil timestamp means no timestamp filter (all historical PENDING rows).
	runIngestionLoop(ctx, d, span, nil)
}

func runBackfillResetGroup(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runBackfillResetGroup")

	ctx, span := tickTracer.Start(ctx, "runBackfillResetGroup")
	defer span.End()

	runResetLoop(ctx, d, span)
}

func runBackfillValidationGroup(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runBackfillValidationGroup")

	ctx, span := tickTracer.Start(ctx, "runBackfillValidationGroup")
	defer span.End()

	// Backfill: validate COMPLETED rows, skipping paused instruments.
	runValidationLoop(ctx, d, span, nil, true)
	// Re-check all NOT_FOUND rows; if data is found, validate and confirm directly.
	runNotFoundRecheckLoop(ctx, d, span)
}

// ── Loop implementations ──────────────────────────────────────────────────────

func tickActiveStateRows(q *ent.StateQuery, filters ...predicate.State) *ent.StateQuery {
	baseFilters := []predicate.State{
		state.JobTypeEQ(state.JobTypeTICK),
		state.HasInstrumentWith(
			instrument.IsPauseEQ(false),
			instrument.IsActiveEQ(true),
		),
		state.IsDeletedEQ(false),
	}
	return q.Where(append(baseFilters, filters...)...)
}

func orderStateStatuses(statuses ...state.Status) func(*sql.Selector) {
	return func(s *sql.Selector) {
		s.OrderExprFunc(func(b *sql.Builder) {
			b.WriteString("CASE status ")
			for idx, status := range statuses {
				b.WriteString("WHEN '")
				b.WriteString(string(status))
				b.WriteString(fmt.Sprintf("' THEN %d ", idx+1))
			}
			b.WriteString(fmt.Sprintf("ELSE %d END", len(statuses)+1))
		})
	}
}

// runIngestionLoop claims PENDING TICK rows one at a time and processes each
// through the ETL pipeline in a goroutine bounded by tickDownloadSem / tickProcessSem.
//
// If the timestamp is non-nil, only rows for that exact hour are claimed (T-0 mode).
// A nil timestamp processes all historical PENDING rows (Backfill mode).
//
// Correctness note: row claims use FOR UPDATE SKIP LOCKED inside the pool
// transaction so parallel backfill groups route work by current status without
// competing for the same row.
func runIngestionLoop(ctx context.Context, d *dal.DAL, span trace.Span, timestamp *time.Time) {
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
			span.RecordError(err)
			log.Printf("ingestion claim: %v", err)
			break
		}

		loopWg.Add(1)
		go func(r *ent.State) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "ticks/ingestion-row")
			executeIngestionETL(ctx, d, r)
		}(claimed)
	}
	loopWg.Wait()
}

// runRecoveryLoop claims PENDING/PROCESSED/FAILED/NOT_FOUND rows for the given
// timestamp and applies the T-1 recovery rules to each.
func runRecoveryLoop(ctx context.Context, d *dal.DAL, span trace.Span, target time.Time) {
	var loopWg sync.WaitGroup
	for ctx.Err() == nil {
		claimed, err := claimStateAsProcessed(ctx, d, func(q *ent.StateQuery) *ent.StateQuery {
			return tickActiveStateRows(q,
				state.StatusIn(
					state.StatusPENDING,
					state.StatusPROCESSED,
					state.StatusFAILED,
					state.StatusNOT_FOUND,
				),
				state.TimestampEQ(target),
			).
				Order(
					// Process stuck PROCESSED first, then FAILED, then PENDING/NOT_FOUND.
					orderStateStatuses(
						state.StatusPROCESSED,
						state.StatusFAILED,
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
			span.RecordError(err)
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

// runValidationLoop claims COMPLETED rows (optionally for a specific timestamp)
// and validates their Parquet files.
//
// A nil timestamp processes all historical COMPLETED rows (Backfill mode).
//
// The respectPause flag controls whether instruments with IsPause=true are skipped:
//   - False: T-2 Regular — only IsActive=true (paused instruments still get
//     their already-completed files validated, per architecture §D.3).
//   - True: Backfill Validation — requires IsActive=true AND IsPause=false,
//     per architecture §E.3.
func runValidationLoop(ctx context.Context, d *dal.DAL, span trace.Span, timestamp *time.Time, respectPause bool) {
	var loopWg sync.WaitGroup
	for ctx.Err() == nil {
		// preclaimPrev is the row's PreviousStatus column value BEFORE the claim
		// update overwrites it. It is used by executeValidation to detect a genuine
		// demotion: a row whose last recorded history was CONFIRMED (i.e., it was
		// confirmed in a prior cycle) that is now found to be BROKEN.
		claimed, preclaimPrev, err := claimStateWithPrevious(ctx, d, func(q *ent.StateQuery) *ent.StateQuery {
			instrumentFilters := []predicate.Instrument{instrument.IsActiveEQ(true)}
			if respectPause {
				instrumentFilters = append(instrumentFilters, instrument.IsPauseEQ(false))
			}
			query := q.
				Where(
					state.JobTypeEQ(state.JobTypeTICK),
					state.StatusEQ(state.StatusCOMPLETED),
					state.HasInstrumentWith(instrumentFilters...),
					state.IsDeletedEQ(false),
				).
				WithInstrument(). // needed to readFromR2 / validateParquetFile
				Order(state.ByTimestamp())
			if timestamp != nil {
				query = query.Where(state.TimestampEQ(*timestamp))
			}
			return query
		}, func(row *ent.State) (state.PreviousStatus, state.Status) {
			originalStatus := row.Status
			newStatus := state.StatusPROCESSED
			if originalStatus == state.StatusPROCESSED {
				// Stray PROCESSED: reset to PENDING immediately, no ETL.
				newStatus = state.StatusPENDING
			}
			return state.PreviousStatus(originalStatus), newStatus
		})
		if err != nil {
			if ent.IsNotFound(err) {
				break
			}
			span.RecordError(err)
			log.Printf("validation claim: %v", err)
			break
		}

		loopWg.Add(1)
		go func(r *ent.State, prev *state.PreviousStatus) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "ticks/validation-row")
			executeValidation(ctx, d, r, prev)
		}(claimed, preclaimPrev)
	}
	loopWg.Wait()
}

// runResetLoop is used by Backfill Reset. It claims PROCESSED/FAILED/BROKEN rows
// and resets them: PROCESSED → PENDING, FAILED/BROKEN → PENDING (retry) or ABANDONED.
// Processing is sequential (no goroutines) since these are fast DB-only operations.
func runResetLoop(ctx context.Context, d *dal.DAL, span trace.Span) {
	for ctx.Err() == nil {
		claimed, err := claimStateAsProcessed(ctx, d, func(q *ent.StateQuery) *ent.StateQuery {
			return tickActiveStateRows(q,
				state.StatusIn(
					state.StatusPROCESSED,
					state.StatusFAILED,
					state.StatusBROKEN,
				),
			).
				Order(
					orderStateStatuses(
						state.StatusPROCESSED,
						state.StatusFAILED,
						state.StatusBROKEN,
					),
					state.ByTimestamp(),
				)
		})
		if err != nil {
			if ent.IsNotFound(err) {
				break
			}
			span.RecordError(err)
			log.Printf("backfill reset claim: %v", err)
			break
		}

		executeResetAction(ctx, d, claimed)
	}
}

// runNotFoundRecheckLoop (Backfill Validation only) claims every NOT_FOUND row
// regardless of its streak and re-checks the API.
// Unlike T-1 recovery (which leaves a found row at COMPLETED for T-2 to validate
// later), Backfill goes all the way to CONFIRMED in a single pass per §E.3:
// Download → convert → upload → validate → CONFIRMED + SyncTask.
func runNotFoundRecheckLoop(ctx context.Context, d *dal.DAL, span trace.Span) {
	var loopWg sync.WaitGroup
	for ctx.Err() == nil {
		claimed, preclaimPrev, err := claimStateWithPrevious(ctx, d, func(q *ent.StateQuery) *ent.StateQuery {
			return tickActiveStateRows(q,
				state.StatusEQ(state.StatusNOT_FOUND),
			).
				WithInstrument(). // needed to downloadBI5 / ETL chain
				Order(state.ByTimestamp())
		}, func(*ent.State) (state.PreviousStatus, state.Status) {
			return state.PreviousStatusNOT_FOUND, state.StatusPROCESSED
		})
		if err != nil {
			if ent.IsNotFound(err) {
				break
			}
			span.RecordError(err)
			log.Printf("NOT_FOUND recheck claim: %v", err)
			break
		}

		loopWg.Add(1)
		go func(r *ent.State, prev *state.PreviousStatus) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "ticks/not-found-recheck-row")
			executeNotFoundRecheckFull(ctx, d, r, prev)
		}(claimed, preclaimPrev)
	}
	loopWg.Wait()
}

// ── ETL execution ─────────────────────────────────────────────────────────────

// executeIngestionETL runs the download → convert → upload pipeline for a row
// that has already been claimed (status = PROCESSED). Updates the row's status
// to COMPLETED, FAILED, or NOT_FOUND (with a Zero-Row flow if streak ≥ 3).
func executeIngestionETL(ctx context.Context, d *dal.DAL, row *ent.State) {
	ctx, span := tickTracer.Start(ctx, "ticks/etl", tickSpanAttrs(row))
	defer span.End()

	// Phase 1: Download BI5 (≤12 concurrent to respect Dukascopy rate limits).
	tickDownloadSem <- struct{}{}
	data, err := downloadBI5(ctx, row)
	<-tickDownloadSem

	if err != nil {
		if isNotFoundError(err) {
			handleNotFoundStreak(ctx, d, row)
		} else {
			span.RecordError(err)
			updateStateFailed(ctx, d, row)
		}
		return
	}

	// Data was found: reset NotFoundStreak to 0 if it was previously non-zero.
	if row.NotFoundStreak > 0 {
		resetNotFoundStreak(ctx, d, row, "reset streak")
	}

	// Phase 2: Convert + Upload (≤50 concurrent to prevent OOM).
	executeConvertUpload(ctx, d, row, data)
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

	case state.PreviousStatusFAILED:
		// Increment retry count atomically; ABANDONED at ≥ 5, else PENDING.
		handleRetryReset(ctx, d, row)

	case state.PreviousStatusNOT_FOUND:
		// Re-check the API; if data is available, run ETL, otherwise increment streak.
		executeNotFoundRecheck(ctx, d, row)
	}
}

// executeNotFoundRecheck re-checks the Dukascopy API for a claimed NOT_FOUND row.
// Used both by T-1 recovery and the Backfill NOT_FOUND recheck loop.
func executeNotFoundRecheck(ctx context.Context, d *dal.DAL, row *ent.State) {
	ctx, span := tickTracer.Start(ctx, "ticks/not-found-recheck", tickSpanAttrs(row))
	defer span.End()

	tickDownloadSem <- struct{}{}
	data, err := downloadBI5(ctx, row)
	<-tickDownloadSem

	if err != nil {
		if isNotFoundError(err) {
			// Still 404: increment streak (may trigger Zero-Row flow).
			handleNotFoundStreak(ctx, d, row)
		} else {
			// System / network error: leave row as NOT_FOUND for the next cycle.
			span.RecordError(err)
			log.Printf("NOT_FOUND recheck error for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
			if e := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
				_, e := tx.State.UpdateOneID(row.ID).
					SetPreviousStatus(state.PreviousStatusPROCESSED).
					SetStatus(state.StatusNOT_FOUND).
					SetUpdatedAt(time.Now().UTC()).
					Save(ctx)
				return e
			}); e != nil {
				span.RecordError(e)
				log.Printf("NOT_FOUND revert for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), e)
			}
		}
		return
	}

	// Data found: reset streak to 0 and run ETL.
	resetNotFoundStreak(ctx, d, row, "reset streak on recovery")

	executeConvertUpload(ctx, d, row, data)
}

// executeNotFoundRecheckFull is the Backfill-Validation variant of the NOT_FOUND
// recheck. Unlike the T-1 variant (executeNotFoundRecheck) which stops at COMPLETED
// and lets T-2 validate later, this function performs the full pipeline in one pass:
// Download → streak-reset → convert → upload → validate → CONFIRMED + SyncTask.
//
// preclaimPrev is the row's PreviousStatus before the current claim, forwarded to
// executeValidation so a genuine demotion from CONFIRMED can trigger a SyncTask.
func executeNotFoundRecheckFull(ctx context.Context, d *dal.DAL, row *ent.State, preclaimPrev *state.PreviousStatus) {
	ctx, span := tickTracer.Start(ctx, "ticks/not-found-recheck-full", tickSpanAttrs(row))
	defer span.End()

	// Step 1: Download (rate-limited — same pool as BI5 downloads per §E.3).
	tickDownloadSem <- struct{}{}
	data, err := downloadBI5(ctx, row)
	<-tickDownloadSem

	if err != nil {
		if isNotFoundError(err) {
			// Still 404: increment streak; may trigger Zero-Row flow.
			handleNotFoundStreak(ctx, d, row)
		} else {
			// System / network error: leave row as NOT_FOUND for the next cycle.
			span.RecordError(err)
			log.Printf("backfill NOT_FOUND recheck for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
			updateSimpleStatus(ctx, d, row,
				state.PreviousStatusPROCESSED, state.StatusNOT_FOUND, "revert to NOT_FOUND")
		}
		return
	}

	// Data found: atomically reset NotFoundStreak to 0 before processing.
	resetNotFoundStreak(ctx, d, row, "reset streak")

	// Step 2: Convert + Upload (rate-limited). Do NOT set status to COMPLETED here;
	// executeValidation (Step 3) will write the final CONFIRMED or BROKEN status.
	var uploadErr error
	func() {
		tickProcessSem <- struct{}{}
		defer func() { <-tickProcessSem }()

		parquet, convErr := convertBI5ToParquet(data, row)
		if convErr != nil {
			span.RecordError(convErr)
			uploadErr = convErr
			return
		}
		if upErr := uploadToR2(ctx, row, parquet); upErr != nil {
			span.RecordError(upErr)
			uploadErr = upErr
		}
	}()

	if uploadErr != nil {
		updateStateFailed(ctx, d, row)
		return
	}

	// Step 3: Validate the just-uploaded file and promote directly to CONFIRMED.
	// The row is still in the PROCESSED state; executeValidation finalizes it.
	executeValidation(ctx, d, row, preclaimPrev)
}

// executeValidation reads the Parquet file from R2 and validates it physically.
// On success the row is promoted to CONFIRMED with a SyncTask event.
// On failure the row is set to BROKEN; a SyncTask is inserted only when
// preclaimPrev == CONFIRMED, which indicates a genuine demotion: the row had
// already been confirmed in a previous cycle and is now found to be BROKEN.
func executeValidation(ctx context.Context, d *dal.DAL, row *ent.State, preclaimPrev *state.PreviousStatus) {
	ctx, span := tickTracer.Start(ctx, "ticks/validate", tickSpanAttrs(row))
	defer span.End()

	fileBytes, err := readFromR2(ctx, row)
	if err != nil {
		span.RecordError(err)
		log.Printf("read from R2 for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
		// Unable to read the file — treat as BROKEN so Backfill can retry.
		updateStateBroken(ctx, d, row)
		return
	}

	validationErr := validateParquetFile(ctx, row, fileBytes)

	if err := updateValidatedTickStatus(ctx, d, row, validationErr, preclaimPrev); err != nil {
		span.RecordError(err)
		log.Printf("validation status update for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
	}
}

func updateValidatedTickStatus(ctx context.Context, d *dal.DAL, row *ent.State, validationErr error, preclaimPrev *state.PreviousStatus) error {
	return d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		if validationErr == nil {
			saved, e := tx.State.UpdateOneID(row.ID).
				SetPreviousStatus(state.PreviousStatusPROCESSED).
				SetStatus(state.StatusCONFIRMED).
				SetUpdatedAt(time.Now().UTC()).
				Save(ctx)
			if e != nil {
				return e
			}
			return upsertSyncTaskInTx(ctx, tx, saved.InstrumentID, saved.Timestamp)
		}

		broken, e := tx.State.UpdateOneID(row.ID).
			SetPreviousStatus(state.PreviousStatusPROCESSED).
			SetStatus(state.StatusBROKEN).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		if e != nil {
			return e
		}
		log.Printf(
			"state_transition status=%s state_id=%s job_type=%s previous_status=%s traceID=%s message=%q",
			state.StatusBROKEN,
			broken.ID,
			broken.JobType,
			state.PreviousStatusPROCESSED,
			trace.SpanFromContext(ctx).SpanContext().TraceID(),
			"tick parquet validation failed",
		)
		if preclaimPrev != nil && *preclaimPrev == state.PreviousStatusCONFIRMED {
			return upsertSyncTaskInTx(ctx, tx, broken.InstrumentID, broken.Timestamp)
		}
		return nil
	})
}

// executeResetAction applies the Backfill Reset rule for a claimed row:
//   - PROCESSED → PENDING (stuck worker recovery)
//   - FAILED / BROKEN → PENDING with AddRetryCount(+1), or ABANDONED if ≥ 5
func executeResetAction(ctx context.Context, d *dal.DAL, row *ent.State) {
	if row.PreviousStatus == nil {
		return
	}
	switch *row.PreviousStatus {
	case state.PreviousStatusPROCESSED:
		resetStateToPending(ctx, d, row)
	case state.PreviousStatusFAILED, state.PreviousStatusBROKEN:
		handleRetryReset(ctx, d, row)
	}
}

// ── State transition helpers ──────────────────────────────────────────────────

func resetNotFoundStreak(ctx context.Context, d *dal.DAL, row *ent.State, logPrefix string) {
	span := trace.SpanFromContext(ctx)
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			SetNotFoundStreak(0).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		span.RecordError(err)
		log.Printf("%s for %s (traceID=%s): %v", logPrefix, row.ID, span.SpanContext().TraceID(), err)
	}
}

// handleNotFoundStreak atomically increments NotFoundStreak.
// If the new value reaches notFoundThreshold (3), it triggers the Zero-Row
// Parquet commit flow. Otherwise, the row is set to NOT_FOUND.
func handleNotFoundStreak(ctx context.Context, d *dal.DAL, row *ent.State) {
	span := trace.SpanFromContext(ctx)

	var updated *ent.State
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		var e error
		// Atomically increment and read the new streak value.
		updated, e = tx.State.UpdateOneID(row.ID).
			AddNotFoundStreak(1).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		if e != nil {
			return e
		}
		if updated.NotFoundStreak < notFoundThreshold {
			// Simple NOT_FOUND: streak below threshold.
			updated, e = tx.State.UpdateOneID(row.ID).
				SetPreviousStatus(state.PreviousStatusPROCESSED).
				SetStatus(state.StatusNOT_FOUND).
				SetUpdatedAt(time.Now().UTC()).
				Save(ctx)
		}
		// else: row stays PROCESSED; commitZeroRowAndConfirm will finalize it.
		return e
	})
	if err != nil {
		span.RecordError(err)
		log.Printf("streak increment for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
		return
	}

	if updated.NotFoundStreak >= notFoundThreshold {
		commitZeroRowAndConfirm(ctx, d, updated)
	}
}

// commitZeroRowAndConfirm implements the Zero-Row Parquet commit protocol:
//  1. Build a zero-row Parquet file in memory.
//  2. Set IsHoliday=true in DB atomically — must happen BEFORE the R2 upload.
//  3. Upload the file to R2 only after the DB commit.
//  4. Read back and physically validate.
//  5. Promote to CONFIRMED + SyncTask (or BROKEN on validation failure).
func commitZeroRowAndConfirm(ctx context.Context, d *dal.DAL, row *ent.State) {
	ctx, span := tickTracer.Start(ctx, "ticks/commit-zero-row", tickSpanAttrs(row))
	defer span.End()

	// 1. Build a zero-row Parquet file in memory.
	zeroRowData := buildZeroRowParquet()

	// 2. Set IsHoliday=true before uploading.
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			SetIsHoliday(true).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		span.RecordError(fmt.Errorf("set IsHoliday for %s: %w", row.ID, err))
		log.Printf("set IsHoliday for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
		return
	}

	// Build a local copy with IsHoliday=true so the R2 path helpers and
	// validator see the correct state without an extra DB round-trip.
	rowWithHoliday := *row
	rowWithHoliday.IsHoliday = true

	// 3. Upload to R2 — only because the DB commit above succeeded.
	if err := uploadToR2(ctx, &rowWithHoliday, zeroRowData); err != nil {
		span.RecordError(fmt.Errorf("upload zero-row for %s: %w", row.ID, err))
		log.Printf("upload zero-row for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
		return
	}

	// 4. Read back and validate.
	fileBytes, err := readFromR2(ctx, &rowWithHoliday)
	if err != nil {
		span.RecordError(fmt.Errorf("read zero-row from R2 for %s: %w", row.ID, err))
		log.Printf("read zero-row from R2 for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
		return
	}
	validationErr := validateParquetFile(ctx, &rowWithHoliday, fileBytes)

	// 5. Finalize status.
	if err := updateValidatedTickStatus(ctx, d, row, validationErr, nil); err != nil {
		span.RecordError(err)
		log.Printf("confirm/break zero-row for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
	}
}

// handleRetryReset increments RetryCount atomically and either resets the row
// to PENDING (for another attempt) or transitions it to ABANDONED (≥ maxRetryCount).
func handleRetryReset(ctx context.Context, d *dal.DAL, row *ent.State) {
	span := trace.SpanFromContext(ctx)
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		// First update: only increment the counter — no status transition, so
		// PreviousStatus must NOT be touched here (architecture rule: update
		// PreviousStatus only together with a status change).
		updated, e := tx.State.UpdateOneID(row.ID).
			AddRetryCount(1).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		if e != nil {
			return e
		}
		newStatus := state.StatusPENDING
		if updated.RetryCount >= maxRetryCount {
			newStatus = state.StatusABANDONED
		}
		// Second update: actual status transition — PreviousStatus is set here.
		_, e = tx.State.UpdateOneID(row.ID).
			SetPreviousStatus(state.PreviousStatusPROCESSED).
			SetStatus(newStatus).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	})
	if err != nil {
		span.RecordError(err)
		log.Printf("retry reset for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
	}
}

func resetStateToPending(ctx context.Context, d *dal.DAL, row *ent.State) {
	updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusPENDING, "reset to PENDING")
}

func updateStateCompleted(ctx context.Context, d *dal.DAL, row *ent.State) {
	updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusCOMPLETED, "set COMPLETED")
}

func updateStateFailed(ctx context.Context, d *dal.DAL, row *ent.State) {
	updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "set FAILED")
}

func updateStateBroken(ctx context.Context, d *dal.DAL, row *ent.State) {
	updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusBROKEN, "set BROKEN")
}

// upsertSyncTaskInTx inserts a PENDING SyncTask for the specified instrument and day,
// or resets an existing one back to PENDING to trigger ResolvedTickCount recomputation.
// It must be called inside an ExecuteInPool transaction.
func upsertSyncTaskInTx(ctx context.Context, tx *ent.Tx, instrumentID uuid.UUID, tickTimestamp time.Time) error {
	targetDate := tickTimestamp.Truncate(24 * time.Hour)

	return tx.ExecContext(ctx, `
		INSERT INTO ingestion.sync_tasks (id, instrument_id, target_date, status, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (instrument_id, target_date)
		DO UPDATE SET status = EXCLUDED.status
	`,
		uuid.New(),
		instrumentID,
		targetDate,
		string(synctask.StatusPENDING),
		time.Now().UTC(),
	)
}

// ── Shared sub-pipeline ───────────────────────────────────────────────────────

// executeConvertUpload acquires tickProcessSem, converts raw BI5 bytes to
// Parquet, uploads to R2, then updates the row to COMPLETED or FAILED.
// Extracted to avoid duplicating the same block in executeIngestionETL and
// executeNotFoundRecheck.
func executeConvertUpload(ctx context.Context, d *dal.DAL, row *ent.State, data []byte) {
	ctx, span := tickTracer.Start(ctx, "ticks/convert-upload", tickSpanAttrs(row))
	defer span.End()

	tickProcessSem <- struct{}{}
	defer func() { <-tickProcessSem }()

	parquet, err := convertBI5ToParquet(data, row)
	if err != nil {
		span.RecordError(err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "convert failed")
		return
	}
	if err := uploadToR2(ctx, row, parquet); err != nil {
		span.RecordError(err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "upload failed")
		return
	}
	updateStateCompleted(ctx, d, row)
}

// updateSimpleStatus writes a single status transition to the DB.
// The prev value is recorded in previous_status; next becomes the new status.
// Extracted to avoid duplicating the same ExecuteInPool pattern across
// updateStateCompleted / updateStateFailed / updateStateBroken / resetStateToPending.
func updateSimpleStatus(ctx context.Context, d *dal.DAL, row *ent.State, prev state.PreviousStatus, next state.Status, errMsg string) {
	span := trace.SpanFromContext(ctx)
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			SetPreviousStatus(prev).
			SetStatus(next).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		span.RecordError(err)
		log.Printf("%s for %s (traceID=%s): %v", errMsg, row.ID, span.SpanContext().TraceID(), err)
		return
	}
	if next == state.StatusFAILED || next == state.StatusBROKEN {
		log.Printf(
			"state_transition status=%s state_id=%s job_type=%s previous_status=%s traceID=%s message=%q",
			next,
			row.ID,
			row.JobType,
			prev,
			span.SpanContext().TraceID(),
			errMsg,
		)
	}
}

// ── External operations ───────────────────────────────────────────────────────
// All functions below require row.Edges.Instrument to be non-nil.
// The claim loops in this file use WithInstrument() to satisfy that contract.

// tickSpanAttrs returns OTel span start options with instrument+timestamp attributes.
// Safe to call even when row.Edges.Instrument is nil.
func tickSpanAttrs(row *ent.State) trace.SpanStartOption {
	attrs := []attribute.KeyValue{
		attribute.String("state.id", row.ID.String()),
		attribute.String("tick.timestamp", row.Timestamp.UTC().Format(time.RFC3339)),
		attribute.String("job.type", string(row.JobType)),
	}
	if row.Edges.Instrument != nil {
		attrs = append(attrs, attribute.String("instrument.name", row.Edges.Instrument.Name))
	}
	return trace.WithAttributes(attrs...)
}

// errNotFound is returned by downloadBI5 when Dukascopy responds with HTTP 404.
var errNotFound = errors.New("tick source: 404 not found")

func isNotFoundError(err error) bool { return errors.Is(err, errNotFound) }

// downloadBI5 downloads the LZMA-compressed BI5 tick file from Dukascopy for
// the instrument and UTC hour represented by row.  Returns errNotFound on 404.
//
// Uses tickDownloadSem (bounded ≤ 12) — must be called while holding a semaphore slot.
func downloadBI5(ctx context.Context, row *ent.State) ([]byte, error) {
	ctx, span := tickTracer.Start(ctx, "ticks/download-bi5", tickSpanAttrs(row))
	defer span.End()

	if dukClient == nil {
		err := errClientsNotInitialized
		span.RecordError(err)
		return nil, err
	}
	if row.Edges.Instrument == nil {
		err := fmt.Errorf("downloadBI5: instrument edge not loaded for state %s", row.ID)
		span.RecordError(err)
		return nil, err
	}
	data, err := dukClient.FetchBI5(ctx, row.Edges.Instrument.Name, row.Timestamp)
	if errors.Is(err, dukascopy.ErrNotFound) {
		span.AddEvent("404: no data for this hour")
		return nil, errNotFound
	}
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return data, nil
}

// convertBI5ToParquet decompresses the raw BI5 bytes, converts each tick to the
// canonical Parquet schema (timestamp/instrument/bid/ask/bid_volume/ask_volume),
// and returns the in-memory Parquet file bytes.
//
// The instrument's Divider field is used to convert integer prices to actual
// decimal prices (e.g., divider=100,000 for EUR/USD, 1,000 for XAU/USD).
func convertBI5ToParquet(raw []byte, row *ent.State) ([]byte, error) {
	if row.Edges.Instrument == nil {
		return nil, fmt.Errorf("convertBI5ToParquet: instrument edge not loaded for state %s", row.ID)
	}
	inst := row.Edges.Instrument

	ticks, err := dukascopy.ParseBI5(raw, row.Timestamp, inst.Divider)
	if err != nil {
		return nil, fmt.Errorf("convertBI5ToParquet: parse BI5: %w", err)
	}

	rows := make([]tickparquet.Row, len(ticks))
	for i, t := range ticks {
		rows[i] = tickparquet.Row{
			Timestamp:  t.Timestamp.UnixMicro(),
			Instrument: inst.Name,
			Bid:        t.Bid,
			Ask:        t.Ask,
			BidVolume:  int64(t.BidVolume),
			AskVolume:  int64(t.AskVolume),
		}
	}
	return tickparquet.Write(rows)
}

// uploadToR2 puts the Parquet bytes at the canonical R2 object path for this row.
//
// Path: ingestion/dukascopy/ticks/{instrument}/{YYYY}/{MM}/ticks-{instrument}-{YYYY-MM-DD}-{HH}.parquet
//
// This implements the Overwrite Policy (§6.2): calling on an existing key
// replaces the old file, which is the correct behavior on FAILED/BROKEN retries.
func uploadToR2(ctx context.Context, row *ent.State, data []byte) error {
	ctx, span := tickTracer.Start(ctx, "ticks/upload-r2", tickSpanAttrs(row))
	defer span.End()

	if r2Client == nil {
		err := errClientsNotInitialized
		span.RecordError(err)
		return err
	}
	if row.Edges.Instrument == nil {
		err := fmt.Errorf("uploadToR2: instrument edge not loaded for state %s", row.ID)
		span.RecordError(err)
		return err
	}
	key := r2.TickObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	if err := r2Client.PutObject(ctx, key, data); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

// readFromR2 downloads the Parquet file for this row from R2 so it can be
// validated. Returns r2.ErrNotFound if the object is absent.
func readFromR2(ctx context.Context, row *ent.State) ([]byte, error) {
	ctx, span := tickTracer.Start(ctx, "ticks/read-r2", tickSpanAttrs(row))
	defer span.End()

	if r2Client == nil {
		err := errClientsNotInitialized
		span.RecordError(err)
		return nil, err
	}
	if row.Edges.Instrument == nil {
		err := fmt.Errorf("readFromR2: instrument edge not loaded for state %s", row.ID)
		span.RecordError(err)
		return nil, err
	}
	key := r2.TickObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	data, err := r2Client.GetObject(ctx, key)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	return data, nil
}

// validateParquetFile runs the four-step physical validation from §6.3:
//  1. The file size is greater than zero, and PAR1 magic bytes are present.
//  2. The Parquet footer can be parsed by parquet-go.
//  3. The schema has six columns in the expected order.
//  4. Each timestamp falls within the expected 1-hour window.
//
// Zero-row files are accepted only if the row is marked as a holiday (step 4 skipped).
func validateParquetFile(ctx context.Context, row *ent.State, data []byte) error {
	_, span := tickTracer.Start(ctx, "ticks/validate-parquet", tickSpanAttrs(row))
	defer span.End()

	err := tickparquet.Validate(data, row.IsHoliday, row.Timestamp.UTC())
	if err != nil {
		span.RecordError(err)
	}
	return err
}

// buildZeroRowParquet returns a valid Parquet file with the Tick schema but
// zero data rows. Used when committing a confirmed market-holiday hour.
func buildZeroRowParquet() []byte {
	data, err := tickparquet.WriteZeroRow()
	if err != nil {
		// WriteZeroRow only fails if the in-memory buffer errors, which should
		// never happen in practice.
		return nil
	}
	return data
}

// recoverGoroutine catches any panic, records it on the active OTel span,
// and logs a fallback message. It must be deferred AFTER wg.Done().
func recoverGoroutine(ctx context.Context, name string) {
	if r := recover(); r != nil {
		span := trace.SpanFromContext(ctx)
		err := fmt.Errorf("panic in %s: %v", name, r)
		span.RecordError(err)
		log.Printf("FATAL: %v (traceID: %s)", err, span.SpanContext().TraceID())
	}
}
