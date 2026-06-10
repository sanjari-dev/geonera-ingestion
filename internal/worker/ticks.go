package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
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
	// tickProcessSem (runtime.NumCPU()) bounding convert+upload. Capping the
	// claim batch at 120 (rather than leaving it unbounded) is what keeps the
	// in-flight PROCESSED-row count, and therefore goroutine/memory growth, predictable.
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

// tickProcessSem limits concurrent Parquet convert+upload goroutines to
// runtime.NumCPU() so the convert stage (CPU-bound) saturates all available
// cores without over-subscribing. Initialised once at package load time.
// Shared across all tick phases.
var tickProcessSem = make(chan struct{}, runtime.NumCPU())

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

	// Guarantee every eligible instrument has a PENDING row for this hour before
	// claiming. In the normal flow maintenance already seeded these rows; this
	// call is a safety net for instruments that were just activated or whose row
	// was not yet seeded by the time T-0 fires. ON CONFLICT DO NOTHING makes it
	// a no-op when the row already exists.
	ensureT0TickTasks(ctx, d, span, target)

	runIngestionLoop(ctx, d, span, &target)
}

// ensureT0TickTasks inserts a PENDING TICK row at target for every instrument
// where IsActive=true AND IsPause=false that does not already have one.
// Each instrument gets exactly one task per hour; duplicates are silently
// ignored via ON CONFLICT (instrument_id, timestamp, job_type) DO NOTHING.
func ensureT0TickTasks(ctx context.Context, d *dal.DAL, span trace.Span, target time.Time) {
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
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
		span.RecordError(err)
		log.Printf("T0 ensure tasks (traceID=%s): %v", span.SpanContext().TraceID(), err)
	}
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
	// T-2 Regular: claims COMPLETED + NOT_FOUND, re-downloads BI5 for cross-validation.
	// COMPLETED → re-download matches → CONFIRMED; NOT_FOUND → streak+1, at ≥3 → CONFIRMED.
	// Only IsActive filter — paused instruments still get validated.
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
	// (RetryCount+1 / NotFoundStreak-1 → PENDING; no ABANDONED cap).
	backfillLayerRetryReset
	// backfillLayerT2Action: COMPLETED or NOT_FOUND (streak >= threshold) → PROCESSED,
	// run the same re-download + cross-validation logic as T-2 (executeT2Action):
	// COMPLETED+2xx → convert+upload+validate → CONFIRMED/BROKEN,
	// NOT_FOUND+2xx → PENDING, NOT_FOUND+404+streak>=3 → validateZeroRow → CONFIRMED,
	// COMPLETED+404 → BROKEN.
	backfillLayerT2Action
	// backfillLayerNotFoundRecheck: NOT_FOUND (streak < threshold) → PROCESSED,
	// re-download and apply T-1-style recheck: 2xx → PENDING, 404 → Zero-Row + NOT_FOUND.
	backfillLayerNotFoundRecheck
	// backfillLayerNone: the row was fully resolved inside the claim transaction
	// itself (e.g., PROCESSED zombie or ABANDONED → PENDING) — no further dispatch needed.
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

	// Claim up to 120 rows once and dispatch in parallel.
	// ABANDONED rows are sorted last, so they are only reached when no other
	// actionable rows exist within the T-3 boundary.
	runBackfillMasterClaim(ctx, d, span)
}


// runBackfillMasterClaim implements the "Master Bulk-Claim" strategy: claim up
// to backfillMasterClaimLimit rows in a single FOR UPDATE SKIP LOCKED transaction
// (oldest Timestamp first, Timestamp <= T-3), apply the per-row routing decision,
// dispatch all rows in parallel goroutines, and wait for them to finish.
// Each backfill trigger processes exactly one batch of up to 120 rows — no loop.
// The only real throttles are the download rate-gate/semaphore (12 concurrent)
// and the convert/upload semaphore (runtime.NumCPU()).
func runBackfillMasterClaim(ctx context.Context, d *dal.DAL, span trace.Span) {
	boundary := backfillTimeBoundary()

	batch, err := claimBackfillMasterBatch(ctx, d, boundary)
	if err != nil {
		if !ent.IsNotFound(err) {
			span.RecordError(err)
			log.Printf("backfill master-claim: %v", err)
		}
		return
	}
	if len(batch) == 0 {
		return
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
}

// claimBackfillMasterBatch performs the single master claim: it locks up to
// backfillMasterClaimLimit rows (FOR UPDATE SKIP LOCKED, restricted to the T-3
// exclusive zone), decides — purely from each row's pre-claim status — both the
// claim transition (PreviousStatus/Status to write) AND the routing layer to
// dispatch it to afterward, applies the transition atomically, and returns the
// routed batch.
//
// Rows are sorted by status priority first (ABANDONED last), then by Timestamp
// ASC (oldest first). This guarantees ABANDONED rows only enter the batch after
// all other statuses at or before the T-3 boundary have been exhausted.
//
// This single function replaces the per-status routing rules that used to be
// split across runIngestionLoop / runResetLoop / runValidationLoop /
// runNotFoundRecheckLoop / runAbandonedResetPhase for the Backfill path.
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
				state.StatusABANDONED,
			),
			state.TimestampLTE(boundary),
		).
			WithInstrument(). // needed by the ETL / validation / recheck pipelines
			Order(orderStateStatuses(
				state.StatusPENDING,
				state.StatusPROCESSED,
				state.StatusFAILED,
				state.StatusBROKEN,
				state.StatusCOMPLETED,
				state.StatusNOT_FOUND,
				// ABANDONED is intentionally omitted — the ELSE clause places it last.
			)).
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
				if row.RetryCount > 5 {
					// Retry budget exhausted — retire to ABANDONED at claim time.
					newPrev, newStatus, layer = state.PreviousStatusFAILED, state.StatusABANDONED, backfillLayerNone
				} else {
					newPrev, newStatus, layer = state.PreviousStatusFAILED, state.StatusPROCESSED, backfillLayerRetryReset
				}

			case state.StatusBROKEN:
				if row.RetryCount > 5 {
					newPrev, newStatus, layer = state.PreviousStatusBROKEN, state.StatusABANDONED, backfillLayerNone
				} else {
					newPrev, newStatus, layer = state.PreviousStatusBROKEN, state.StatusPROCESSED, backfillLayerRetryReset
				}

			case state.StatusCOMPLETED:
				// Layer C — T-2 style: re-download + cross-validate, same as T-2.
				newPrev, newStatus, layer = state.PreviousStatusCOMPLETED, state.StatusPROCESSED, backfillLayerT2Action

			case state.StatusNOT_FOUND:
				if row.NotFoundStreak >= notFoundThreshold {
					// Layer C — T-2 style: re-download + cross-validate, same as T-2.
					newPrev, newStatus, layer = state.PreviousStatusNOT_FOUND, state.StatusPROCESSED, backfillLayerT2Action
				} else {
					// Layer B: streak below threshold — re-download like T-1.
					// 2xx → PENDING, 404 → Zero-Row + NOT_FOUND, error → FAILED.
					newPrev, newStatus, layer = state.PreviousStatusNOT_FOUND, state.StatusPROCESSED, backfillLayerNotFoundRecheck
				}

			case state.StatusABANDONED:
				// ABANDONED rows only reach this branch after all other statuses
				// are exhausted (status-priority sort puts them last). Reset with
				// RetryCount=0 so they get a clean ingestion cycle.
				updated, ue := tx.State.UpdateOneID(row.ID).
					SetPreviousStatus(state.PreviousStatusABANDONED).
					SetStatus(state.StatusPENDING).
					SetRetryCount(0).
					SetUpdatedAt(time.Now().UTC()).
					Save(ctx)
				if ue != nil {
					return ue
				}
				updated.Edges = row.Edges
				routed = append(routed, backfillRoutedRow{
					row:          updated,
					preclaimPrev: preclaimPrev,
					layer:        backfillLayerNone,
				})
				continue

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
		executeIngestionETL(ctx, d, item.row, handleNotFoundIncrement)
	case backfillLayerRetryReset:
		handleRetryReset(ctx, d, item.row)
	case backfillLayerT2Action:
		executeT2Action(ctx, d, item.row, item.preclaimPrev)
	case backfillLayerNotFoundRecheck:
		executeNotFoundRecheck(ctx, d, item.row)
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

// runAbandonedResetPhase is the last-resort reset shared by the Candle and any
// future job-type backfill paths. After all FAILED/BROKEN rows are exhausted it
// iterates over every ABANDONED row for the given job type and resets it to
// PENDING with RetryCount=0, giving the row a fresh retry budget.
//
// Processing is sequential (FOR UPDATE SKIP LOCKED, one row per transaction)
// because this phase runs only after all higher-priority work is done — there is
// no concurrency to exploit and serialising avoids lock contention.
func runAbandonedResetPhase(ctx context.Context, d *dal.DAL, span trace.Span, jobType state.JobType) {
	for ctx.Err() == nil {
		err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			row, err := tx.State.Query().Where(
				state.JobTypeEQ(jobType),
				state.StatusEQ(state.StatusABANDONED),
				state.HasInstrumentWith(
					instrument.IsActiveEQ(true),
					instrument.IsPauseEQ(false),
				),
				state.IsDeletedEQ(false),
			).Order(state.ByTimestamp()).FirstForUpdateSkipLocked(ctx)
			if err != nil {
				return err
			}
			_, err = tx.State.UpdateOneID(row.ID).
				SetPreviousStatus(state.PreviousStatusABANDONED).
				SetStatus(state.StatusPENDING).
				SetRetryCount(0).
				SetUpdatedAt(time.Now().UTC()).
				Save(ctx)
			return err
		})
		if err != nil {
			if ent.IsNotFound(err) {
				break
			}
			span.RecordError(err)
			log.Printf("abandoned reset (%s, traceID=%s): %v", jobType, span.SpanContext().TraceID(), err)
			break
		}
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
// tickProcessSem (runtime.NumCPU() concurrent convert+upload) — not by an
// artificial goroutine-pool cap here.
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
			executeIngestionETL(ctx, d, r, handleNotFoundSimple)
		}(claimed)
	}
	loopWg.Wait()
}

// runRecoveryLoop claims PENDING/PROCESSED/FAILED/BROKEN/NOT_FOUND rows for the
// given timestamp and applies the T-1 recovery rules to each.
func runRecoveryLoop(ctx context.Context, d *dal.DAL, span trace.Span, target time.Time) {
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

// runValidationLoop claims COMPLETED and NOT_FOUND rows for the given hour
// and cross-validates each one via executeT2Action (re-download from Dukascopy):
//
//   - COMPLETED  + 2xx  → convert+upload(overwrite)+validate → CONFIRMED or BROKEN.
//   - NOT_FOUND  + 2xx  → SetNotFoundStreak=0, Status=PENDING (ETL next cycle).
//   - NOT_FOUND  + 404  → streak+1; at notFoundThreshold(3): executeValidation → CONFIRMED.
//   - COMPLETED  + 404  → BROKEN (anomaly: data was present at T-0 time).
//   - Any non-404 error → FAILED.
//
// Used by T-2 (two-hours-ago) with a non-nil timestamp.
// Backfill drives its own rows via the Master Bulk-Claim.
// respectPause=false for T-2 Regular: paused instruments still get validated.
func runValidationLoop(ctx context.Context, d *dal.DAL, span trace.Span, timestamp *time.Time, respectPause bool) {
	var loopWg sync.WaitGroup
	for ctx.Err() == nil {
		// preclaimPrev is the row's PreviousStatus BEFORE the claim — forwarded to
		// executeNotFoundRecheckFull to detect genuine demotions (CONFIRMED → BROKEN).
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
			span.RecordError(err)
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

// ── ETL execution ─────────────────────────────────────────────────────────────

// executeIngestionETL runs the download → convert → upload pipeline for a row
// that has already been claimed (status = PROCESSED). Updates the row's status
// to COMPLETED, FAILED, or NOT_FOUND.
//
// onNotFound controls how the streak is recorded on a 404 response:
//   - T-0 passes handleNotFoundSimple (SetNotFoundStreak=1, always first attempt)
//   - Backfill Group 1 passes handleNotFoundIncrement (AddNotFoundStreak+1, streak may be >0)
func executeIngestionETL(ctx context.Context, d *dal.DAL, row *ent.State, onNotFound func(context.Context, *dal.DAL, *ent.State)) {
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
			onNotFound(ctx, d, row)
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

	// Phase 2: Convert + Upload — semaphore capped at runtime.NumCPU().
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

	case state.PreviousStatusFAILED, state.PreviousStatusBROKEN:
		// Increment RetryCount, decrement NotFoundStreak (floor 0), always → PENDING.
		handleRetryReset(ctx, d, row)

	case state.PreviousStatusNOT_FOUND:
		// Re-check API: data → PENDING+streak=0; still 404 → Zero-Row+NOT_FOUND.
		executeNotFoundRecheck(ctx, d, row)
	}
}

// executeNotFoundRecheck re-checks the Dukascopy API for a claimed NOT_FOUND row.
// Used exclusively by T-1 recovery (executeRecoveryAction).
//
// Data found  → SetNotFoundStreak=0, Status=PENDING (ETL on the next cycle).
// Still 404   → AddNotFoundStreak(+1, streak=2), IsHoliday=true, upload Zero-Row Parquet, Status=NOT_FOUND.
// Other error → FAILED (handled by backfill-reset on retry).
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
			// Still 404: increment streak, mark holiday, upload Zero-Row Parquet.
			// Status stays NOT_FOUND — validation (→ CONFIRMED) is deferred to Backfill.
			// DB commit happens before R2 upload (safe ordering: holiday flag persists
			// even if the upload fails and is retried on the next Backfill cycle).
			if dbErr := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
				_, e := tx.State.UpdateOneID(row.ID).
					AddNotFoundStreak(1).
					SetIsHoliday(true).
					SetPreviousStatus(state.PreviousStatusPROCESSED).
					SetStatus(state.StatusNOT_FOUND).
					SetUpdatedAt(time.Now().UTC()).
					Save(ctx)
				return e
			}); dbErr != nil {
				span.RecordError(dbErr)
				log.Printf("not-found recheck: DB update for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), dbErr)
				return
			}
			zeroRow := buildZeroRowParquet()
			if upErr := uploadToR2(ctx, row, zeroRow); upErr != nil {
				span.RecordError(upErr)
				log.Printf("not-found recheck: Zero-Row upload for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), upErr)
			}
		} else {
			span.RecordError(err)
			log.Printf("NOT_FOUND recheck error for %s (traceID=%s): %v — setting FAILED", row.ID, span.SpanContext().TraceID(), err)
			updateStateFailed(ctx, d, row)
		}
		return
	}

	// Data is available again: reset streak to 0, set PENDING.
	// The row will be picked up for a full ETL pass on the next cycle.
	_ = data
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			SetNotFoundStreak(0).
			SetPreviousStatus(state.PreviousStatusNOT_FOUND).
			SetStatus(state.StatusPENDING).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		span.RecordError(err)
		log.Printf("not-found recheck: reset to PENDING for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
	}
}

// executeT2Action implements the T-2 cross-validation pipeline.
// Both COMPLETED and NOT_FOUND rows are re-downloaded from Dukascopy for verification.
//
//   COMPLETED + 2xx  → convert + upload (overwrite) + validate → CONFIRMED or BROKEN.
//   NOT_FOUND + 2xx  → SetNotFoundStreak=0, Status=PENDING (ETL deferred to next cycle).
//   NOT_FOUND + 404  → AddNotFoundStreak(+1); streak≥3 → commitZeroRowAndConfirm → CONFIRMED.
//   COMPLETED + 404  → BROKEN (data was present at T-0 but is now missing — anomaly).
//   Non-404 error    → FAILED.
func executeT2Action(ctx context.Context, d *dal.DAL, row *ent.State, preclaimPrev *state.PreviousStatus) {
	ctx, span := tickTracer.Start(ctx, "ticks/T2-action", tickSpanAttrs(row))
	defer span.End()

	select {
	case <-tickDownloadRateGate:
	case <-ctx.Done():
		return
	}
	tickDownloadSem <- struct{}{}
	data, err := downloadBI5(ctx, row)
	<-tickDownloadSem

	wasNotFound := row.PreviousStatus != nil && *row.PreviousStatus == state.PreviousStatusNOT_FOUND

	if err != nil {
		if isNotFoundError(err) {
			if wasNotFound {
				// NOT_FOUND + 404: increment streak.
				// At ≥ notFoundThreshold: validate the Zero-Row Parquet that T-1 already
				// uploaded to R2 → CONFIRMED (same validation path as COMPLETED+2xx).
				// Below threshold: set NOT_FOUND and wait for the next cycle.
				newStreak := row.NotFoundStreak + 1
				newRetryCount := row.RetryCount - 1
				if newRetryCount < 0 {
					newRetryCount = 0
				}
				if newStreak >= notFoundThreshold {
					if dbErr := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
						_, e := tx.State.UpdateOneID(row.ID).
							SetNotFoundStreak(newStreak).
							SetRetryCount(newRetryCount).
							SetUpdatedAt(time.Now().UTC()).
							Save(ctx)
						return e
					}); dbErr != nil {
						span.RecordError(dbErr)
						log.Printf("T2: streak update for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), dbErr)
						return
					}
					row.NotFoundStreak = newStreak
					executeValidation(ctx, d, row, preclaimPrev)
				} else {
					if dbErr := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
						_, e := tx.State.UpdateOneID(row.ID).
							SetNotFoundStreak(newStreak).
							SetRetryCount(newRetryCount).
							SetPreviousStatus(state.PreviousStatusPROCESSED).
							SetStatus(state.StatusNOT_FOUND).
							SetUpdatedAt(time.Now().UTC()).
							Save(ctx)
						return e
					}); dbErr != nil {
						span.RecordError(dbErr)
						log.Printf("T2: NOT_FOUND update for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), dbErr)
					}
				}
			} else {
				// COMPLETED + unexpected 404: data was there at T-0 time — mark as anomaly.
				span.RecordError(err)
				log.Printf("T2: COMPLETED got 404 for %s (traceID=%s) — setting BROKEN", row.ID, span.SpanContext().TraceID())
				updateStateBroken(ctx, d, row)
			}
		} else {
			span.RecordError(err)
			log.Printf("T2: download error for %s (traceID=%s): %v — setting FAILED", row.ID, span.SpanContext().TraceID(), err)
			updateStateFailed(ctx, d, row)
		}
		return
	}

	if wasNotFound {
		// NOT_FOUND + data now available: reset streak, hand off to next cycle via PENDING.
		if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			_, e := tx.State.UpdateOneID(row.ID).
				SetNotFoundStreak(0).
				SetPreviousStatus(state.PreviousStatusNOT_FOUND).
				SetStatus(state.StatusPENDING).
				SetUpdatedAt(time.Now().UTC()).
				Save(ctx)
			return e
		}); err != nil {
			span.RecordError(err)
			log.Printf("T2: NOT_FOUND→PENDING for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
		}
		return
	}

	// COMPLETED + 2xx: full convert + upload (overwrite) + validate → CONFIRMED.
	if row.NotFoundStreak > 0 {
		resetNotFoundStreak(ctx, d, row, "T2 reset streak")
	}
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
	executeValidation(ctx, d, row, preclaimPrev)
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

// handleNotFoundSimple sets NotFoundStreak = 1 and status = NOT_FOUND.
// Used exclusively by T-0: a PENDING row in T-0 is always freshly seeded,
// so its streak is guaranteed to be 0. Setting to 1 is equivalent to +1
// and makes the first-attempt intent explicit.
// Unlike handleNotFoundStreak, it never touches RetryCount and never triggers
// the Zero-Row / commitZeroRowAndConfirm flow.
func handleNotFoundSimple(ctx context.Context, d *dal.DAL, row *ent.State) {
	span := trace.SpanFromContext(ctx)
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			SetNotFoundStreak(1).
			SetPreviousStatus(state.PreviousStatusPROCESSED).
			SetStatus(state.StatusNOT_FOUND).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		span.RecordError(err)
		log.Printf("not-found simple for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
	}
}

// handleNotFoundIncrement increments NotFoundStreak by 1 and sets status = NOT_FOUND.
// Used by Backfill Group 1: a PENDING row in backfill may have been reclaimed from
// a NOT_FOUND path (streak > 0), so Add is required instead of Set.
// Like handleNotFoundSimple, it never touches RetryCount and never triggers Zero-Row.
func handleNotFoundIncrement(ctx context.Context, d *dal.DAL, row *ent.State) {
	span := trace.SpanFromContext(ctx)
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			AddNotFoundStreak(1).
			SetPreviousStatus(state.PreviousStatusPROCESSED).
			SetStatus(state.StatusNOT_FOUND).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		span.RecordError(err)
		log.Printf("not-found increment for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
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
// handleRetryReset increments RetryCount and resets the row to PENDING.
// Bidirectional counter-rule: retry penalty reduces NotFoundStreak (floor 0),
// mirroring how handleNotFoundStreak decrements RetryCount on 404 evidence.
// T-1 recovery never transitions to ABANDONED — retry is unbounded.
func handleRetryReset(ctx context.Context, d *dal.DAL, row *ent.State) {
	span := trace.SpanFromContext(ctx)

	newStreak := row.NotFoundStreak - 1
	if newStreak < 0 {
		newStreak = 0
	}

	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			AddRetryCount(1).
			SetNotFoundStreak(newStreak).
			SetPreviousStatus(state.PreviousStatusFAILED).
			SetStatus(state.StatusPENDING).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
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
