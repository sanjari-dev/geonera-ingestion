package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/instrument"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
)

var candleTracer = otel.Tracer("worker/candles")

// RunCandleParentHandler is the single entry point for all candle-aggregation work.
// It acquires LockIDCandle so Regular and Backfill modes cannot run concurrently.
//
// mode must be either "REGULAR" or "BACKFILL".
func RunCandleParentHandler(ctx context.Context, mode string, d *dal.DAL) {
	ctx, span := candleTracer.Start(ctx, fmt.Sprintf("RunCandleParentHandler_%s", mode))
	defer span.End()

	tx, err := d.AcquireAdvisoryLock(ctx)
	if err != nil {
		span.RecordError(err)
		log.Printf("candles %s: acquire lock (traceID=%s): %v", mode, span.SpanContext().TraceID(), err)
		return
	}
	defer func() { _ = tx.Rollback() }()

	var locked bool
	err = tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", dal.LockIDCandle).Scan(&locked)
	if err != nil || !locked {
		span.AddEvent("LockIDCandle already held by another instance, skipping")
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
					span.RecordError(fmt.Errorf("candle lock heartbeat lost: %w", err))
					cancelETL()
					return
				}
			}
		}
	}()

	var wg sync.WaitGroup
	switch mode {
	case "REGULAR":
		wg.Add(1)
		go runCandleRegular(lockCtx, d, &wg)
	case "BACKFILL":
		wg.Add(1)
		go runCandleBackfill(lockCtx, d, &wg)
	default:
		log.Printf("candles: unknown mode %q", mode)
	}
	wg.Wait()
}

// runCandleRegular implements §4.F:
//  1. Seed: upsert a CANDLE state row for today 00:00:00 UTC (Status=PENDING) for
//     every active instrument. Idempotent — ent.IsConstraintError means already seeded.
//  2. Aggregation: claim PENDING CANDLE rows where ResolvedTickCount=24 and run
//     the full stream-aggregation pipeline per executeCandleAggregation.
func runCandleRegular(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runCandleRegular")

	ctx, span := candleTracer.Start(ctx, "candles/regular")
	defer span.End()

	// Step 1 — Seed today's CANDLE row for all active instruments.
	seedTodayCandleRows(ctx, d, span)

	// Step 2 — Claim PENDING CANDLE rows with ResolvedTickCount=24 and aggregate.
	runCandleAggregationLoop(ctx, d, span)
}

// runCandleBackfill implements §4.G:
//  1. Reset: claim PROCESSED/FAILED/BROKEN CANDLE rows and reset them to PENDING
//     (incrementing RetryCount for FAILED/BROKEN; ABANDONED at ≥ 5).
//  2. Aggregation: same pipeline as Regular Step 2.
func runCandleBackfill(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runCandleBackfill")

	ctx, span := candleTracer.Start(ctx, "candles/backfill")
	defer span.End()

	// Step 1 — Reset stuck/failed rows.  Must finish before aggregation begins
	// (same reasoning as Tick Backfill: Reset and Ingestion cannot run concurrently).
	runCandleResetLoop(ctx, d, span)

	// Step 2 — Aggregate any PENDING CANDLE rows that are now ready.
	runCandleAggregationLoop(ctx, d, span)
}

// ── Seeding ───────────────────────────────────────────────────────────────────

// seedTodayCandleRows queries every active instrument and attempts to insert a
// CANDLE state row for today 00:00:00 UTC with Status=PENDING.
// A constraint error means the row already exists — silently skip it.
func seedTodayCandleRows(ctx context.Context, d *dal.DAL, span trace.Span) {
	today := time.Now().UTC().Truncate(24 * time.Hour)

	// Fetch all active instruments (read-only, no transaction needed).
	var instruments []*ent.Instrument
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		var e error
		instruments, e = tx.Instrument.Query().
			Where(instrument.IsActiveEQ(true)).
			All(ctx)
		return e
	}); err != nil {
		span.RecordError(err)
		log.Printf("candle seed: list instruments (traceID=%s): %v", span.SpanContext().TraceID(), err)
		return
	}

	for _, instr := range instruments {
		if ctx.Err() != nil {
			return
		}
		seedOneCandleRow(ctx, d, span, instr.ID, today)
	}
}

// seedOneCandleRow attempts to insert a CANDLE PENDING row for (instrumentID, today).
// Constraint errors are silently ignored (idempotent seed — row already exists).
func seedOneCandleRow(ctx context.Context, d *dal.DAL, span trace.Span, instrumentID uuid.UUID, today time.Time) {
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		return tx.State.Create().
			SetInstrumentID(instrumentID).
			SetJobType(state.JobTypeCANDLE).
			SetTimestamp(today).
			SetStatus(state.StatusPENDING).
			SetResolvedTickCount(0).
			SetRetryCount(0).
			SetNotFoundStreak(0).
			SetIsHoliday(false).
			SetIsDeleted(false).
			SetUpdatedAt(time.Now().UTC()).
			Exec(ctx)
	}); err != nil {
		if !ent.IsConstraintError(err) {
			span.RecordError(err)
			log.Printf("candle seed %s %s (traceID=%s): %v", instrumentID, today.Format(time.DateOnly), span.SpanContext().TraceID(), err)
		}
		// constraint error = row already exists; silently skip
	}
}

// ── Aggregation loop ──────────────────────────────────────────────────────────

// runCandleAggregationLoop claims PENDING CANDLE rows where ResolvedTickCount=24
// and dispatches each to executeCandleAggregation in a goroutine.
// The loop uses the same serial-claim pattern as tick workers: because
// LockIDCandle is held, no other candle process competes for these rows.
func runCandleAggregationLoop(ctx context.Context, d *dal.DAL, span trace.Span) {
	var loopWg sync.WaitGroup
	for ctx.Err() == nil {
		var claimed *ent.State
		err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			row, err := tx.State.Query().
				Where(
					state.JobTypeEQ(state.JobTypeCANDLE),
					state.StatusEQ(state.StatusPENDING),
					state.ResolvedTickCount(24),
					state.HasInstrumentWith(instrument.IsActiveEQ(true)),
					state.IsDeletedEQ(false),
				).
				Order(state.ByTimestamp()).
				First(ctx)
			if err != nil {
				return err
			}
			claimed, err = tx.State.UpdateOneID(row.ID).
				SetPreviousStatus(state.PreviousStatus(row.Status)).
				SetStatus(state.StatusPROCESSED).
				SetUpdatedAt(time.Now().UTC()).
				Save(ctx)
			return err
		})
		if err != nil {
			if ent.IsNotFound(err) {
				break
			}
			span.RecordError(err)
			log.Printf("candle aggregation claim (traceID=%s): %v", span.SpanContext().TraceID(), err)
			break
		}

		loopWg.Add(1)
		go func(r *ent.State) {
			defer loopWg.Done()
			executeCandleAggregation(ctx, d, r)
		}(claimed)
	}
	loopWg.Wait()
}

// ── Reset loop ────────────────────────────────────────────────────────────────

// runCandleResetLoop (Backfill only) claims PROCESSED/FAILED/BROKEN CANDLE rows
// and resets them:
//   - PROCESSED → PENDING (stuck-worker recovery, no retry increment)
//   - FAILED / BROKEN → PENDING + AddRetryCount(+1), or ABANDONED at ≥ maxRetryCount
//
// Processing is sequential (no goroutines): these are fast DB-only operations.
func runCandleResetLoop(ctx context.Context, d *dal.DAL, span trace.Span) {
	for ctx.Err() == nil {
		var claimed *ent.State
		err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			row, err := tx.State.Query().
				Where(
					state.JobTypeEQ(state.JobTypeCANDLE),
					state.StatusIn(
						state.StatusPROCESSED,
						state.StatusFAILED,
						state.StatusBROKEN,
					),
					state.HasInstrumentWith(instrument.IsActiveEQ(true)),
					state.IsDeletedEQ(false),
				).
				Order(state.ByTimestamp()).
				First(ctx)
			if err != nil {
				return err
			}
			claimed, err = tx.State.UpdateOneID(row.ID).
				SetPreviousStatus(state.PreviousStatus(row.Status)).
				SetStatus(state.StatusPROCESSED).
				SetUpdatedAt(time.Now().UTC()).
				Save(ctx)
			return err
		})
		if err != nil {
			if ent.IsNotFound(err) {
				break
			}
			span.RecordError(err)
			log.Printf("candle reset claim (traceID=%s): %v", span.SpanContext().TraceID(), err)
			break
		}

		executeCandleRetryReset(ctx, d, claimed)
	}
}

// ── Aggregation pipeline ──────────────────────────────────────────────────────

// executeCandleAggregation is the main candle aggregation pipeline (§4.F step 2):
//  1. Query the 24 CONFIRMED TICK state rows for this instrument+day.
//  2. Stream each hourly Parquet from R2 into the OHLCV accumulator (O(1) memory).
//     - Skip hours where tickState.IsHoliday == true (Zero-Row tick file).
//     - Handle NoSuchKey: IsDeleted=true → safe skip; IsDeleted=false → BROKEN.
//  3. Build the daily Candles Parquet from the accumulator.
//  4. Upload to R2, validate, promote to CONFIRMED.
//     On any error: PROCESSED → FAILED or BROKEN depending on cause.
func executeCandleAggregation(ctx context.Context, d *dal.DAL, row *ent.State) {
	ctx, span := candleTracer.Start(ctx, "candles/aggregation",
		trace.WithAttributes(
			attribute.String("state.id", row.ID.String()),
			attribute.String("candle.date", row.Timestamp.UTC().Format("2006-01-02")),
		),
	)
	defer span.End()

	// Step 1 — Query 24 CONFIRMED TICK rows for this instrument+day.
	dayStart := row.Timestamp.UTC().Truncate(24 * time.Hour)
	dayEnd := dayStart.Add(24 * time.Hour)

	var tickStates []*ent.State
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		var e error
		tickStates, e = tx.State.Query().
			Where(
				state.JobTypeEQ(state.JobTypeTICK),
				state.StatusEQ(state.StatusCONFIRMED),
				state.InstrumentID(row.InstrumentID),
				state.TimestampGTE(dayStart),
				state.TimestampLT(dayEnd),
				state.IsDeletedEQ(false),
			).
			Order(state.ByTimestamp()).
			All(ctx)
		return e
	}); err != nil {
		span.RecordError(err)
		log.Printf("candle agg %s: query tick states (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "query tick states failed")
		return
	}

	// Step 2 — Stream each hourly Parquet from R2 into the OHLCV accumulator.
	acc := newOHLCVAccumulator()
	broken := false

	for _, tickState := range tickStates {
		if ctx.Err() != nil {
			return
		}

		// §4.F-c: skip Zero-Row files (market holidays).
		if tickState.IsHoliday {
			continue
		}

		data, err := readTickParquetFromR2(ctx, tickState)
		if err != nil {
			if isNoSuchKeyError(err) {
				// §4.F-d: NoSuchKey handling.
				if tickState.IsDeleted {
					// Pruned row — safe to skip.
					continue
				}
				// BROKEN Storage: file should exist but doesn't.
				span.RecordError(fmt.Errorf("candle agg %s: NoSuchKey for tick %s (not deleted): BROKEN", row.ID, tickState.ID))
				log.Printf("candle agg %s: NoSuchKey for tick %s (not deleted) (traceID=%s)", row.ID, tickState.ID, span.SpanContext().TraceID())
				broken = true
				break
			}
			span.RecordError(err)
			log.Printf("candle agg %s: read tick parquet %s (traceID=%s): %v", row.ID, tickState.ID, span.SpanContext().TraceID(), err)
			updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "read tick parquet failed")
			return
		}

		if err := acc.AccumulateTickParquet(data); err != nil {
			span.RecordError(err)
			log.Printf("candle agg %s: accumulate tick %s (traceID=%s): %v", row.ID, tickState.ID, span.SpanContext().TraceID(), err)
			updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "accumulate tick parquet failed")
			return
		}
	}

	if broken {
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusBROKEN, "broken storage: tick parquet missing")
		return
	}

	// Step 3 — Build the daily Candles Parquet (all 19 timeframes).
	candleData, err := buildCandleParquet(ctx, row, acc)
	if err != nil {
		span.RecordError(err)
		log.Printf("candle agg %s: build candle parquet (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "build candle parquet failed")
		return
	}

	// Step 4a — Upload to R2.
	if err := uploadCandleParquetToR2(ctx, row, candleData); err != nil {
		span.RecordError(err)
		log.Printf("candle agg %s: upload candle parquet (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "upload candle parquet failed")
		return
	}

	// Step 4b — Read back and validate.
	stored, err := readCandleParquetFromR2(ctx, row)
	if err != nil {
		span.RecordError(err)
		log.Printf("candle agg %s: read back candle parquet (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusBROKEN, "read-back candle parquet failed")
		return
	}

	if err := validateCandleParquet(ctx, row, stored); err != nil {
		span.RecordError(err)
		log.Printf("candle agg %s: validate candle parquet (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusBROKEN, "validate candle parquet failed")
		return
	}

	// Step 4c — Promote to CONFIRMED (§4.F: PROCESSED → CONFIRMED directly).
	updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusCONFIRMED, "set candle CONFIRMED")
}

// ── Reset action ──────────────────────────────────────────────────────────────

// executeCandleRetryReset applies the Backfill Reset rule for a claimed CANDLE row:
//   - PROCESSED → PENDING (stuck-worker recovery; no RetryCount increment)
//   - FAILED / BROKEN → PENDING + AddRetryCount(+1), or ABANDONED at ≥ maxRetryCount
func executeCandleRetryReset(ctx context.Context, d *dal.DAL, row *ent.State) {
	if row.PreviousStatus == nil {
		return
	}
	switch *row.PreviousStatus {
	case state.PreviousStatusPROCESSED:
		// Stuck PROCESSED: reset directly to PENDING, no retry increment.
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusPENDING, "candle reset stuck PROCESSED")

	case state.PreviousStatusFAILED, state.PreviousStatusBROKEN:
		// Increment RetryCount first, then decide PENDING vs ABANDONED.
		candleHandleRetryReset(ctx, d, row)
	}
}

// candleHandleRetryReset increments RetryCount atomically and either resets the
// row to PENDING (for another attempt) or transitions it to ABANDONED (≥ maxRetryCount).
// Mirrors handleRetryReset in ticks.go but scoped to CANDLE rows.
func candleHandleRetryReset(ctx context.Context, d *dal.DAL, row *ent.State) {
	span := trace.SpanFromContext(ctx)
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		// First update: only increment the counter — no status transition.
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
		log.Printf("candle retry reset for %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
	}
}

// ── External operation stubs ──────────────────────────────────────────────────
// These require external client implementations (R2 SDK, Parquet library).
// Until wired up they return errNotImplemented so claimed rows end up FAILED
// (recoverable by Backfill) rather than crashing the process.

// errNoSuchKey mirrors r2.ErrNotFound so that candle aggregation can detect
// a missing tick Parquet and apply the NoSuchKey handling rules from §4.F.
var errNoSuchKey = r2.ErrNotFound

func isNoSuchKeyError(err error) bool { return errors.Is(err, r2.ErrNotFound) }

// readTickParquetFromR2 fetches the hourly tick Parquet for the given TICK state row.
// Returns errNoSuchKey when the R2 object is absent.
func readTickParquetFromR2(_ context.Context, _ *ent.State) ([]byte, error) {
	// TODO: implement Cloudflare R2 (S3-compatible) GetObject for tick Parquet path
	return nil, fmt.Errorf("readTickParquetFromR2: %w", errNotImplemented)
}

// buildCandleParquet assembles a daily Candles Parquet file (all 19 timeframes)
// from the OHLCV accumulator built by streaming the 24 hourly tick Parquets.
func buildCandleParquet(_ context.Context, _ *ent.State, _ *ohlcvAccumulator) ([]byte, error) {
	// TODO: implement Parquet writer for candle data (19 timeframes)
	return nil, fmt.Errorf("buildCandleParquet: %w", errNotImplemented)
}

// uploadCandleParquetToR2 puts the daily Candle Parquet bytes at the canonical
// R2 object path for this candle row.
func uploadCandleParquetToR2(_ context.Context, _ *ent.State, _ []byte) error {
	// TODO: implement Cloudflare R2 (S3-compatible) PutObject for candle Parquet path
	return fmt.Errorf("uploadCandleParquetToR2: %w", errNotImplemented)
}

// readCandleParquetFromR2 fetches the daily Candle Parquet from R2 for validation.
func readCandleParquetFromR2(_ context.Context, _ *ent.State) ([]byte, error) {
	// TODO: implement Cloudflare R2 (S3-compatible) GetObject for candle Parquet path
	return nil, fmt.Errorf("readCandleParquetFromR2: %w", errNotImplemented)
}

// validateCandleParquet runs physical validation on the Candle Parquet bytes:
// magic number, schema, timeframe coverage, OHLCV boundary checks.
func validateCandleParquet(_ context.Context, _ *ent.State, _ []byte) error {
	// TODO: implement Parquet validation for candle schema
	return fmt.Errorf("validateCandleParquet: %w", errNotImplemented)
}

// ── OHLCV accumulator stub ────────────────────────────────────────────────────

// ohlcvAccumulator accumulates raw tick Parquet rows into OHLCV candles for
// all 19 supported timeframes. The stub holds no state until the real
// implementation is wired in.
type ohlcvAccumulator struct{}

// newOHLCVAccumulator returns a fresh, empty accumulator.
func newOHLCVAccumulator() *ohlcvAccumulator { return &ohlcvAccumulator{} }

// AccumulateTickParquet feeds one hourly tick Parquet file into the accumulator.
// Rows are processed in-place so memory is O(1) relative to the number of files.
func (a *ohlcvAccumulator) AccumulateTickParquet(_ []byte) error {
	return fmt.Errorf("AccumulateTickParquet: %w", errNotImplemented)
}
