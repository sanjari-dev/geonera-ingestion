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

	// dukascopyMaxRPS is the global maximum requests-per-second sent to Dukascopy.
	// Aligned with dukascopyBurst (= concurrency cap) so every semaphore slot can
	// fire once per second in a steady state.  This prevents 503 thundering-herds
	// without slowing down the pipeline: a fresh worker saturates all 12 slots in
	// the first second, then sustains 12 downloads/s.
	dukascopyMaxRPS = 12

	// dukascopyBurst is the token-bucket burst capacity — equals the concurrency cap,
	// so the burst and sustained rate are identical (12 per second).
	dukascopyBurst = dukascopyMaxRPS

	// backfillMasterClaimLimit is the batch size for the Backfill "Master
	// Bulk-Claim" (Mapping State.md §2): a one FOR UPDATE SKIP LOCKED query locks
	// up to this many rows per cycle, ordered oldest-Timestamp-first, and the
	// program routes them in memory to the Ingestion / Reset / Validation
	// layers based on each row's pre-claim status.
	//
	// There is intentionally NO separate goroutine-pool cap (the old
	// tickLoopMaxGoroutines / tickLoopSem = 48 design) on top of this: every
	// claimed row is dispatched immediately, and the real bottleneck —
	// Dukascopy BI5 downloads — is already throttled to dukascopyBurst (12)
	// concurrent requests via tickDownloadSem / tickDownloadRateGate, with
	// tickProcessSem (50) bounding convert+upload. Capping the claim batch at
	// 120 (rather than leaving it unbounded) is what keeps the in-flight
	// PROCESSED-row count, and therefore goroutine/memory growth, predictable.
	backfillMasterClaimLimit = 120

	// backfillExclusionHours is the "Zona Eksklusif" boundary (Mapping
	// State.md §2): Backfill only ever claims rows timestamped at or before
	// (current hour − this many hours) — i.e., T-3 and older — leaving the
	// most recent hours to the T-0/T-1/T-2 Regular pipeline.
	backfillExclusionHours = 3
)

// tickDownloadSem limits Dukascopy BI5 HTTP downloads (and API re-checks)
// to dukascopyBurst goroutines to prevent connection overload.
// Shared across all tick phases (T-0, T-1, T-2, Backfill).
var tickDownloadSem = make(chan struct{}, dukascopyBurst)

// tickDownloadRateGate is a token-bucket rate limiter for Dukascopy downloads.
// A background goroutine (started by InitDownloadRateLimiter) refills it at
// dukascopyMaxRPS tokens/second. Each download must receive one token before
// acquiring tickDownloadSem, capping the actual request rate even when all
// semaphore slots are free and requests return quickly (e.g., 503 errors).
var tickDownloadRateGate = make(chan struct{}, dukascopyBurst)

// InitDownloadRateLimiter pre-fills the token bucket and starts the background
// goroutine that replenishes it at dukascopyMaxRPS tokens/second.
// Must be called once at startup, before any worker goroutine is dispatched.
func InitDownloadRateLimiter() {
	// Pre-fill bucket so the first burst of dukascopyBurst downloads starts immediately.
	for i := 0; i < dukascopyBurst; i++ {
		select {
		case tickDownloadRateGate <- struct{}{}:
		default:
		}
	}
	interval := time.Second / dukascopyMaxRPS
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			select {
			case tickDownloadRateGate <- struct{}{}:
			default: // bucket full — drop token (prevents stale burst after idle)
			}
		}
	}()
	log.Printf("worker: download rate limiter started — max %d req/s, burst %d, interval %s",
		dukascopyMaxRPS, dukascopyBurst, interval)
}

// tickProcessSem limits concurrent Parquet convert+upload goroutines to 50
// to prevent OOM spikes on high-volume instruments like EURUSD.
// Shared across all tick phases.
var tickProcessSem = make(chan struct{}, 50)

// RunTickParentHandler is the single entry point for all tick-ingestion work.
// It holds the global advisory lock for the full duration, so no other job
// (candles, maintenance, sync) can run concurrently. If the lock is already
//
//	held, the trigger is dropped immediately.
//
// Mode must be either "REGULAR" or "BACKFILL".
func RunTickParentHandler(ctx context.Context, mode string, d *dal.DAL, onStarted func()) bool {
	ctx, span := tickTracer.Start(ctx, fmt.Sprintf("RunTickParentHandler_%s", mode))
	defer span.End()

	return runWithLocks(ctx, d, "ticks/"+mode, []int64{dal.LockIDTick}, onStarted, func(lockCtx context.Context) {
		var wg sync.WaitGroup
		switch mode {
		case "REGULAR":
			wg.Add(3)
			go runT0Phase(lockCtx, d, &wg)
			go runT1Phase(lockCtx, d, &wg)
			go runT2Phase(lockCtx, d, &wg)
		case "BACKFILL":
			wg.Add(1)
			go runBackfillMasterLoop(lockCtx, d, &wg)
		default:
			log.Printf("ticks: unknown mode %q", mode)
		}
		wg.Wait()
	})
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
	// Per Mapping State.md §1.C, T-2 also claims NOT_FOUND rows for its hour and
	// re-checks them against the API (data released → CONFIRMED, still empty →
	// streak handling / Zero-Row at a threshold) — not just COMPLETED rows.
	runValidationLoop(ctx, d, span, &target, false)
}

// ── Backfill: Master Bulk-Claim ───────────────────────────────────────────────
//
// Per Mapping State.md §2, Backfill no longer runs three independent claim
// loops competing for rows under a shared goroutine-pool cap. Instead, it
// performs ONE master claim per cycle — FOR UPDATE SKIP LOCKED, the oldest
// Timestamp first, limited to backfillMasterClaimLimit (120) rows, restricted
// to the "Zona Eksklusif" (Timestamp <= T-3) — and routes the claimed batch to
// the Ingestion / Reset / Validation layers entirely in memory.

// backfillLayer identifies which handler a master-claimed row should be
// dispatched to once the claim transaction (which already performed the
// status transition) has committed.
type backfillLayer int

const (
	// backfillLayerIngestion: PENDING → PROCESSED, run the full download/convert/upload ETL.
	backfillLayerIngestion backfillLayer = iota
	// backfillLayerRetryReset: FAILED/BROKEN → PROCESSED, apply handleRetryReset
	// (RetryCount+1 / NotFoundStreak-1 → PENDING, or ABANDONED at the retry ceiling).
	backfillLayerRetryReset
	// backfillLayerValidation: COMPLETED → PROCESSED, validate the Parquet file.
	backfillLayerValidation
	// backfillLayerRecheck: NOT_FOUND (streak >= threshold) → PROCESSED, run the
	// full single-pass recheck (download → convert → upload → validate → CONFIRMED).
	backfillLayerRecheck
	// backfillLayerNone: the row was fully resolved inside the claim transaction
	// itself (e.g., PROCESSED zombie or NOT_FOUND-with-low streak → PENDING) —
	// no further dispatch needed.
	backfillLayerNone
)

// backfillRoutedRow pairs a freshly master-claimed row with its routing
// decision and the PreviousStatus value recorded immediately before the claim
// overwrote it (forwarded to validation/recheck so a genuine CONFIRMED→BROKEN
// demotion can be detected and a SyncTask emitted).
type backfillRoutedRow struct {
	row          *ent.State
	preclaimPrev *state.PreviousStatus
	layer        backfillLayer
}

// backfillTimeBoundary returns the "Zona Eksklusif" cutoff (Mapping State.md
// §2): Backfill claims only rows timestamped at or before (current hour − 3h),
// leaving the most recent hours exclusively to the T-0/T-1/T-2 Regular pipeline.
func backfillTimeBoundary() time.Time {
	return time.Now().UTC().Truncate(time.Hour).Add(-backfillExclusionHours * time.Hour)
}

func runBackfillMasterLoop(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runBackfillMasterLoop")

	ctx, span := tickTracer.Start(ctx, "runBackfillMasterLoop")
	defer span.End()

	// Phase A: fast bulk reset for orphaned PROCESSED (prev=PENDING) — inexpensive
	// up-front sweep, so the master claim below isn't spent one-row-at-a-time
	// on rows whose outcome is already known (see runBackfillBulkOrphanReset).
	runBackfillBulkOrphanReset(ctx, d, span)

	// Core sweep: master-claim batches of up to 120 rows and route in memory.
	runBackfillMasterClaimLoop(ctx, d, span)

	// Phase C: last-resort reset for ABANDONED rows, once nothing else is actionable.
	runAbandonedResetPhase(ctx, d, span, state.JobTypeTICK)
}

// runBackfillBulkOrphanReset is Phased A of the backfill sweep: a fast bulk
// UPDATE that resets orphaned PROCESSED rows (previous_status = PENDING — i.e.,
// an ingestion goroutine was in-flight when the service restarted or the
// advisory lock expired) back to PENDING in batches of 500, without spending a
// master-claim slot on each one individually.
func runBackfillBulkOrphanReset(ctx context.Context, d *dal.DAL, span trace.Span) {
	const batchSize = 500
	for ctx.Err() == nil {
		var affected int
		err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			ids, e := tx.State.Query().
				Where(
					state.JobTypeEQ(state.JobTypeTICK),
					state.StatusEQ(state.StatusPROCESSED),
					state.PreviousStatusEQ(state.PreviousStatusPENDING),
					state.HasInstrumentWith(instrument.IsActiveEQ(true)),
					state.IsDeletedEQ(false),
				).
				Limit(batchSize).
				IDs(ctx)
			if e != nil || len(ids) == 0 {
				affected = len(ids)
				return e
			}
			affected = len(ids)
			return tx.State.Update().
				Where(state.IDIn(ids...)).
				SetStatus(state.StatusPENDING).
				SetUpdatedAt(time.Now().UTC()).
				Exec(ctx)
		})
		if err != nil {
			span.RecordError(err)
			log.Printf("backfill batch orphan reset: %v", err)
			break
		}
		if affected > 0 {
			log.Printf("backfill: batch-reset %d orphaned PROCESSED→PENDING rows", affected)
		}
		if affected < batchSize {
			break // no more orphans
		}
	}
}

// runBackfillMasterClaimLoop implements the documented "Master Bulk-Claim"
// strategy (Mapping State.md §2): repeatedly lock up to backfillMasterClaimLimit
// rows in a single FOR UPDATE SKIP LOCKED query (the oldest Timestamp first, Timestamp
// <= T-3), apply the per-row claim transition based on each row's pre-claim
// status, and dispatch the routed batch to its handler — all without an
// artificial goroutine-pool cap. The only real throttles are the download
// rate-gate/semaphore (12 concurrent) and the convert/upload semaphore (50).
func runBackfillMasterClaimLoop(ctx context.Context, d *dal.DAL, span trace.Span) {
	boundary := backfillTimeBoundary()

	for ctx.Err() == nil {
		batch, err := claimBackfillMasterBatch(ctx, d, boundary)
		if err != nil {
			if ent.IsNotFound(err) {
				break
			}
			span.RecordError(err)
			log.Printf("backfill master-claim: %v", err)
			break
		}
		if len(batch) == 0 {
			break // nothing left at or before the T-3 boundary
		}

		var batchWg sync.WaitGroup
		for _, item := range batch {
			batchWg.Add(1)
			go func(it backfillRoutedRow) {
				defer batchWg.Done()
				defer recoverGoroutine(ctx, "ticks/backfill-master-row")
				dispatchBackfillRoutedRow(ctx, d, it)
			}(item)
		}
		batchWg.Wait()

		if len(batch) < backfillMasterClaimLimit {
			break // drained the current window — the next trigger will pick up newer rows
		}
	}
}

// claimBackfillMasterBatch performs the single master claim: it locks up to
// backfillMasterClaimLimit rows (FOR UPDATE SKIP LOCKED, oldest Timestamp
// first, restricted to the T-3 exclusive zone), decides — purely from each
// row's pre-claim status — both the claim transition (PreviousStatus/Status to
// write) AND the routing layer to dispatch it to afterward, applies the
// transition atomically, and returns the routed batch.
//
// This single function replaces the per-status routing rules that used to be
// split across runIngestionLoop / runResetLoop / runValidationLoop /
// runNotFoundRecheckLoop for the Backfill path.
func claimBackfillMasterBatch(ctx context.Context, d *dal.DAL, boundary time.Time) ([]backfillRoutedRow, error) {
	var routed []backfillRoutedRow
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		rows, e := tickActiveStateRows(tx.State.Query(),
			state.StatusIn(
				state.StatusPENDING,
				state.StatusPROCESSED,
				state.StatusFAILED,
				state.StatusBROKEN,
				state.StatusCOMPLETED,
				state.StatusNOT_FOUND,
			),
			state.TimestampLTE(boundary),
		).
			WithInstrument(). // needed by the ETL / validation / recheck pipelines
			Order(state.ByTimestamp()).
			ManyForUpdateSkipLocked(ctx, backfillMasterClaimLimit)
		if e != nil {
			return e
		}
		if len(rows) == 0 {
			return &ent.NotFoundError{}
		}

		for _, row := range rows {
			preclaimPrev := row.PreviousStatus
			originalStatus := row.Status

			var newPrev state.PreviousStatus
			var newStatus state.Status
			var layer backfillLayer

			switch originalStatus {
			case state.StatusPENDING:
				// Layer A — Ingesti Utama: download & process the lagging row.
				newPrev, newStatus, layer = state.PreviousStatusPENDING, state.StatusPROCESSED, backfillLayerIngestion

			case state.StatusPROCESSED:
				// Zombie row from a previous in-flight attempt. Per Mapping
				// State.md §2.B, PROCESSED zombies go straight back to PENDING
				// with no counter-mutation — resolved entirely at claim time.
				newPrev, newStatus, layer = state.PreviousStatusPROCESSED, state.StatusPENDING, backfillLayerNone

			case state.StatusFAILED:
				// Layer B — Reset & Pemulihan: apply the retry/abandon rule.
				newPrev, newStatus, layer = state.PreviousStatusFAILED, state.StatusPROCESSED, backfillLayerRetryReset

			case state.StatusBROKEN:
				newPrev, newStatus, layer = state.PreviousStatusBROKEN, state.StatusPROCESSED, backfillLayerRetryReset

			case state.StatusCOMPLETED:
				// Layer C — Validasi Final: validate the uploaded Parquet file.
				newPrev, newStatus, layer = state.PreviousStatusCOMPLETED, state.StatusPROCESSED, backfillLayerValidation

			case state.StatusNOT_FOUND:
				if row.NotFoundStreak >= notFoundThreshold {
					// Layer C: streak already at/over threshold — go straight to
					// the single-pass recheck-and-confirm (or Zero-Row) flow.
					newPrev, newStatus, layer = state.PreviousStatusNOT_FOUND, state.StatusPROCESSED, backfillLayerRecheck
				} else {
					// Layer B: streak still below threshold — simply requeue for
					// a fresh download attempt next cycle. No counter-mutation
					// (Mapping State.md §2.B: "... PENDING").
					newPrev, newStatus, layer = state.PreviousStatusNOT_FOUND, state.StatusPENDING, backfillLayerNone
				}

			default:
				continue
			}

			updated, ue := tx.State.UpdateOneID(row.ID).
				SetPreviousStatus(newPrev).
				SetStatus(newStatus).
				SetUpdatedAt(time.Now().UTC()).
				Save(ctx)
			if ue != nil {
				return ue
			}
			updated.Edges = row.Edges

			routed = append(routed, backfillRoutedRow{
				row:          updated,
				preclaimPrev: preclaimPrev,
				layer:        layer,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return routed, nil
}

// dispatchBackfillRoutedRow runs the handler selected for a row at master-claim
// time. Rows resolved entirely inside the claim transaction (backfillLayerNone)
// require no further action here.
func dispatchBackfillRoutedRow(ctx context.Context, d *dal.DAL, item backfillRoutedRow) {
	switch item.layer {
	case backfillLayerIngestion:
		executeIngestionETL(ctx, d, item.row)
	case backfillLayerRetryReset:
		handleRetryReset(ctx, d, item.row)
	case backfillLayerValidation:
		executeValidation(ctx, d, item.row, item.preclaimPrev)
	case backfillLayerRecheck:
		executeNotFoundRecheckFull(ctx, d, item.row, item.preclaimPrev)
	case backfillLayerNone:
		// Already fully resolved inside the claim transaction.
	}
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

// runIngestionLoop claims PENDING TICK rows one at a time for the given hour
// and processes each through the ETL pipeline in its own goroutine.
//
// Used by T-0 (current-hour ingestion) with a non-nil timestamp — at most one
// row per active instrument, so the natural fan-out is small. Backfill no
// longer drives this loop; it is PENDING rows are claimed and dispatched as part
// of the Master Bulk-Claim (see runBackfillMasterClaimLoop / claimBackfillMasterBatch).
//
// Concurrency is bounded by the real bottlenecks deeper in the pipeline —
// tickDownloadSem / tickDownloadRateGate (12 concurrent downloads) and
// tickProcessSem (50 concurrent convert+upload) — not by an artificial
// goroutine-pool cap here.
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

// runValidationLoop claims COMPLETED and NOT_FOUND rows for the given hour and
// finalizes each one — physical Parquet validation for COMPLETED, and a full
// API recheck (→ CONFIRMED / streak handling / Zero-Row) for NOT_FOUND, per
// the T-2 transition table in Mapping State.md §1.C ("juga menyapu ... Status
// IN ('COMPLETED', 'NOT_FOUND')").
//
// Used by T-2 (two-hours-ago physical validation) with a non-nil timestamp —
// at most one row per active instrument, so the natural fan-out is small.
// Backfill no longer drives this loop; its COMPLETED/NOT_FOUND rows are
// claimed and dispatched as part of the Master Bulk-Claim (see
// runBackfillMasterClaimLoop / claimBackfillMasterBatch).
//
// The respectPause flag controls whether instruments with IsPause=true are
// skipped — T-2 Regular passes false: only IsActive=true (paused instruments
// still get their already-completed files validated, per architecture §D.3).
func runValidationLoop(ctx context.Context, d *dal.DAL, span trace.Span, timestamp *time.Time, respectPause bool) {
	var loopWg sync.WaitGroup
	for ctx.Err() == nil {
		// preclaimPrev is the row's PreviousStatus column value BEFORE the claim
		// update overwrites it. It is used by executeValidation/executeNotFoundRecheckFull
		// to detect a genuine demotion: a row whose last recorded history was
		// CONFIRMED (i.e., it was confirmed in a prior cycle) that is now BROKEN.
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
				WithInstrument(). // needed to readFromR2 / validateParquetFile / downloadBI5
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
			// Route by the row's pre-claim status (now recorded in PreviousStatus):
			// NOT_FOUND → full API recheck; COMPLETED → physical Parquet validation.
			if r.PreviousStatus != nil && *r.PreviousStatus == state.PreviousStatusNOT_FOUND {
				executeNotFoundRecheckFull(ctx, d, r, prev)
				return
			}
			executeValidation(ctx, d, r, prev)
		}(claimed, preclaimPrev)
	}
	loopWg.Wait()
}

// runAbandonedResetPhase is a Phase C — the absolute last resort — of the
// backfill reset pipeline.
//
// It fires at the end of runBackfillMasterLoop (TICK) and runCandleResetLoop
// (CANDLE), strictly AFTER the master-claim sweep has drained every other
// actionable status (PENDING, PROCESSED, NOT_FOUND, FAILED, BROKEN, COMPLETED).
// ABANDONED rows are deliberately the lowest priority: they already exhausted
// their retry budget once, so they must never compete with — or starve — fresh
// work that still has a normal recovery path.
//
// Condition: if every active row for jobType is either CONFIRMED or ABANDONED
// (count of PENDING/PROCESSED/FAILED/BROKEN/NOT_FOUND/COMPLETED == 0) — i.e.,
// the master claim above found nothing left to do — then ABANDONED rows are
// reset to PENDING with RetryCount=0, in batches of backfillMasterClaimLimit
// (120) so the reset moves in the same 120-row increments as the rest of the
// backfill pipeline rather than one large 500-row sweep.
//
// Why RetryCount=0: ABANDONED rows previously exhausted their 5 retries. Resetting
// to 0 gives them a full fresh cycle rather than immediately re-abandoning them.
// If they fail again, they will naturally accumulate retries and be abandoned again.
//
// After the reset, the maintenance gap-fill will see freshly updated PENDING rows
// (UpdatedAt=now, not stale), conclude there are no stuck rows, and clear IsPause.
// The next backfill trigger will then process them.
func runAbandonedResetPhase(ctx context.Context, d *dal.DAL, span trace.Span, jobType state.JobType) {
	if ctx.Err() != nil {
		return
	}

	// Count rows that still need work (everything except CONFIRMED and ABANDONED).
	var actionable int
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		count, e := tx.State.Query().
			Where(
				state.JobTypeEQ(jobType),
				state.StatusIn(
					state.StatusPENDING,
					state.StatusPROCESSED,
					state.StatusFAILED,
					state.StatusBROKEN,
					state.StatusNOT_FOUND,
					state.StatusCOMPLETED,
				),
				state.HasInstrumentWith(instrument.IsActiveEQ(true)),
				state.IsDeletedEQ(false),
			).
			Count(ctx)
		actionable = count
		return e
	}); err != nil {
		span.RecordError(err)
		log.Printf("backfill abandoned-check %s (traceID=%s): %v", jobType, span.SpanContext().TraceID(), err)
		return
	}

	if actionable > 0 {
		// Still rows to process; Phase C must not run yet.
		return
	}

	// All non-abandoned rows are CONFIRMED. Reset ABANDONED → PENDING in batches
	// of backfillMasterClaimLimit (120) — the same unit the master-claim uses
	// for its main sweep, so the whole backfill pipeline moves in consistent
	// 120-row increments end to end (Mapping State.md: "...as much 120 task").
	const batchSize = backfillMasterClaimLimit
	var totalReset int
	for ctx.Err() == nil {
		var affected int
		err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			ids, e := tx.State.Query().
				Where(
					state.JobTypeEQ(jobType),
					state.StatusEQ(state.StatusABANDONED),
					state.HasInstrumentWith(instrument.IsActiveEQ(true)),
					state.IsDeletedEQ(false),
				).
				Limit(batchSize).
				IDs(ctx)
			if e != nil || len(ids) == 0 {
				affected = len(ids)
				return e
			}
			affected = len(ids)
			return tx.State.Update().
				Where(state.IDIn(ids...)).
				SetStatus(state.StatusPENDING).
				SetPreviousStatus(state.PreviousStatusABANDONED).
				SetRetryCount(0).
				SetUpdatedAt(time.Now().UTC()).
				Exec(ctx)
		})
		if err != nil {
			span.RecordError(err)
			log.Printf("backfill abandoned-reset %s (traceID=%s): %v", jobType, span.SpanContext().TraceID(), err)
			break
		}
		totalReset += affected
		if affected < batchSize {
			break
		}
	}

	if totalReset > 0 {
		log.Printf("backfill: last-resort reset %d ABANDONED→PENDING %s rows (retry_count=0); maintenance will clear IsPause on next cycle", totalReset, jobType)
	}
}

// ── ETL execution ─────────────────────────────────────────────────────────────

// executeIngestionETL runs the download → convert → upload pipeline for a row
// that has already been claimed (status = PROCESSED). Updates the row's status
// to COMPLETED, FAILED, or NOT_FOUND (with a Zero-Row flow if streak ≥ 3).
func executeIngestionETL(ctx context.Context, d *dal.DAL, row *ent.State) {
	ctx, span := tickTracer.Start(ctx, "ticks/etl", tickSpanAttrs(row))
	defer span.End()

	log.Printf("ticks: executing ETL for %s tick %s", row.Edges.Instrument.Name, row.Timestamp.Format(time.RFC3339))

	// Phase 1: Download BI5 — two-layer throttle:
	//   1. Rate gate (token bucket): caps global throughput at dukascopyMaxRPS req/s.
	//      Prevents 503 flood when requests fail quickly (e.g., rate-limited responses).
	//   2. Concurrency semaphore: caps simultaneous in-flight connections to dukascopyBurst.
	select {
	case <-tickDownloadRateGate:
	case <-ctx.Done():
		return
	}
	tickDownloadSem <- struct{}{}
	data, err := downloadBI5(ctx, row)
	<-tickDownloadSem

	if err != nil {
		if isNotFoundError(err) {
			handleNotFoundStreak(ctx, d, row)
		} else {
			span.RecordError(err)
			// For rows that have exhausted most retries, a persistent non-404 error
			// (e.g., 503 CDN unavailable) is treated as "data not at source".
			// After maxRetryCount-2 failures, the URL is very unlikely to ever return
			// data; incrementing notFoundStreak lets the zero-row flow resolve the slot
			// rather than letting retry_count exhaust and set ABANDONED.
			//
			// The threshold (maxRetryCount-2 = 3) gives the server 3 genuine retries
			// before treating persistent 503s the same as 404s.
			if row.RetryCount >= maxRetryCount-2 {
				log.Printf("ingestion: %v for %s (retry=%d, traceID=%s) — treating persistent non-404 as not-found, incrementing streak",
					err, row.ID, row.RetryCount, span.SpanContext().TraceID())
				handleNotFoundStreak(ctx, d, row)
			} else {
				updateStateFailed(ctx, d, row)
			}
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

	// Two-layer throttle: rate gate first, then concurrency semaphore.
	select {
	case <-tickDownloadRateGate:
	case <-ctx.Done():
		return
	}
	tickDownloadSem <- struct{}{}
	data, err := downloadBI5(ctx, row)
	<-tickDownloadSem

	if err != nil {
		if isNotFoundError(err) {
			// Still 404: increment streak (may trigger Zero-Row flow).
			handleNotFoundStreak(ctx, d, row)
		} else {
			// System / network error (e.g., 503, timeout): set FAILED instead of NOT_FOUND.
			// Reverting unconditionally to NOT_FOUND risks overwriting a CONFIRMED state
			// that another goroutine may have already set, triggering a confirmation loop.
			// FAILED is picked up by the backfill-reset loop and retried safely.
			span.RecordError(err)
			log.Printf("NOT_FOUND recheck error for %s (traceID=%s): %v — setting FAILED", row.ID, span.SpanContext().TraceID(), err)
			updateStateFailed(ctx, d, row)
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

	// Step 1: Download — two-layer throttle (rate gate + concurrency semaphore).
	select {
	case <-tickDownloadRateGate:
	case <-ctx.Done():
		return
	}
	tickDownloadSem <- struct{}{}
	data, err := downloadBI5(ctx, row)
	<-tickDownloadSem

	if err != nil {
		if isNotFoundError(err) {
			// Still 404: increment streak; may trigger Zero-Row flow.
			handleNotFoundStreak(ctx, d, row)
		} else {
			// Non-404 error (503, timeout) during backfill NOT_FOUND recheck.
			//
			// This row was already NOT_FOUND before the recheck claim. A 503 here
			// means the Dukascopy CDN returned a server error for a URL that was
			// previously a 404 — the data almost certainly does not exist at a source.
			// Treating it as FAILED would consume a retry_count slot and eventually
			// lead to ABANDONED without the zero-row flow ever firing.
			//
			// Instead: increment notFoundStreak (same as a 404 would do). If the
			// streak reaches notFoundThreshold (3), commitZeroRowAndConfirm fires
			// and the slot is permanently resolved as a market-holiday hour.
			//
			// Race safety: the master-claim transaction already flipped this row's
			// Status to PROCESSED (see claimBackfillMasterBatch's "recheck" routing),
			// so it no longer matches any claim filter — T-0/T-1/T-2 target different
			// timestamp windows, and the next backfill master-claim only matches the
			// status set IN (PENDING, PROCESSED, FAILED, BROKEN, COMPLETED, NOT_FOUND)
			// with PreviousStatus left untouched here. No other goroutine can steal
			// this row between the claim transaction and this handleNotFoundStreak call.
			span.RecordError(err)
			log.Printf("backfill NOT_FOUND recheck for %s (traceID=%s): %v — treating as not-found, incrementing streak", row.ID, span.SpanContext().TraceID(), err)
			handleNotFoundStreak(ctx, d, row)
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
			var instName string
			if row.Edges.Instrument != nil {
				instName = row.Edges.Instrument.Name
			} else {
				instName = saved.InstrumentID.String()
			}
			log.Printf("ticks: successfully validated and CONFIRMED %s tick %s", instName, saved.Timestamp.Format(time.RFC3339))
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
// Bidirectional counter-rule: 404 → notFoundStreak++, retryCount-- (floor 0).
func handleNotFoundStreak(ctx context.Context, d *dal.DAL, row *ent.State) {
	span := trace.SpanFromContext(ctx)

	// 404 evidence reduces the retry penalty (floor 0).
	newRetryCount := row.RetryCount - 1
	if newRetryCount < 0 {
		newRetryCount = 0
	}

	var updated *ent.State
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		var e error
		// Atomically increment streak and decrement retryCount.
		updated, e = tx.State.UpdateOneID(row.ID).
			AddNotFoundStreak(1).
			SetRetryCount(newRetryCount).
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
		// UpdateOneID.Save() does not load edges — carry the instrument edge
		// from the original row, so commitZeroRowAndConfirm can compute the R2 key.
		updated.Edges = row.Edges
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
//
// Mutual Exclusive Mutator (Mapping State.md Aturan Global #3 — WAJIB):
// whenever RetryCount is incremented, NotFoundStreak MUST be decremented in
// the same atomic transaction (floor 0), mirroring how handleNotFoundStreak
// decrements RetryCount whenever NotFoundStreak is incremented. This keeps the
// two error counters from accumulating independently across overlapping
// failure modes (e.g., a row that alternates between 404s and 503s).
func handleRetryReset(ctx context.Context, d *dal.DAL, row *ent.State) {
	span := trace.SpanFromContext(ctx)
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		// Bidirectional counter-rule: retry penalty reduces the not-found credit (floor 0).
		newStreak := row.NotFoundStreak - 1
		if newStreak < 0 {
			newStreak = 0
		}

		// First update: only mutate the counters — no status transition, so
		// PreviousStatus must NOT be touched here (architecture rule: update
		// PreviousStatus only together with a status change).
		updated, e := tx.State.UpdateOneID(row.ID).
			AddRetryCount(1).
			SetNotFoundStreak(newStreak).
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
		upd := tx.State.UpdateOneID(row.ID).
			SetPreviousStatus(prev).
			SetStatus(next).
			SetUpdatedAt(time.Now().UTC())
		if next == state.StatusFAILED {
			// Bidirectional counter-rule: non-404 failure → notFoundStreak-- (floor 0).
			// retryCount++ is handled by the reset loop (handleRetryReset).
			newStreak := row.NotFoundStreak - 1
			if newStreak < 0 {
				newStreak = 0
			}
			upd = upd.SetNotFoundStreak(newStreak)
		}
		_, e := upd.Save(ctx)
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
// Must be called while holding BOTH a tickDownloadRateGate token AND a
// tickDownloadSem slot (acquired by the callers in ticks.go).
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
