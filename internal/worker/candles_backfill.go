package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
)

const (
	candleBackfillClaimLimit    = 120
	candleBackfillExclusionDays = 2
	candleMaxRetryCount         = 5
)

type candleBackfillLayer int

const (
	candleLayerNone       candleBackfillLayer = iota
	candleLayerAggregate                      // Layer 1: full aggregation pipeline
	candleLayerReset                          // Layer 2: retry-reset (FAILED/BROKEN)
	candleLayerValidation                     // Layer 3: read-back and validate orphaned COMPLETED
)

type candleRoutedRow struct {
	row   *ent.State
	layer candleBackfillLayer
}

// runCandleBackfill implements §4.G: Master Bulk-Claim strategy.
// One shot per trigger — no loop.
// Claims up to 120 CANDLE rows outside the Zona Eksklusif (Timestamp <= D-2),
// routes each atomically to Layer 1/2/3, dispatches in parallel goroutines,
// then runs Phase C (ABANDONED reset) only when the batch was empty.
func runCandleBackfill(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runCandleBackfill")

	ctx, span := candleTracer.Start(ctx, "candles/backfill")
	defer span.End()

	boundary := backfillCandleBoundary()

	batch, err := claimCandleBackfillBatch(ctx, d, boundary)
	if err != nil {
		span.RecordError(err)
		log.Printf("candle backfill: master claim (traceID=%s): %v", span.SpanContext().TraceID(), err)
		return
	}

	var batchWg sync.WaitGroup
	for _, item := range batch {
		batchWg.Add(1)
		go func(it candleRoutedRow) {
			defer batchWg.Done()
			defer recoverGoroutine(ctx, "candles/backfill-row")
			dispatchCandleBackfillRow(ctx, d, it)
		}(item)
	}
	batchWg.Wait()

	// Phase C: last-resort ABANDONED reset — only when no actionable rows remain.
	if len(batch) == 0 {
		resetCandleAbandonedRows(ctx, d, span, boundary)
	}
}

// ── Backfill helpers ──────────────────────────────────────────────────────────

// backfillCandleBoundary returns the exclusive zone upper boundary for Candle
// Backfill: Timestamp must be <= midnight of (today − 2 days) so that D-1 and D
// are never touched by Backfill (those belong to Candles Regular at 05:08 UTC).
func backfillCandleBoundary() time.Time {
	return time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -candleBackfillExclusionDays)
}

// claimCandleBackfillBatch is the single master claim for Candle Backfill.
// It locks up to candleBackfillClaimLimit rows in one FOR UPDATE SKIP LOCKED
// transaction, decides the routing layer from each row's pre-claim status, applies
// the status transition atomically, and returns the routed batch.
func claimCandleBackfillBatch(ctx context.Context, d *dal.DAL, boundary time.Time) ([]candleRoutedRow, error) {
	var routed []candleRoutedRow
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		rows, e := candleStateRows(tx.State.Query(),
			state.Or(
				state.StatusIn(
					state.StatusFAILED,
					state.StatusBROKEN,
					state.StatusPROCESSED,
					state.StatusCOMPLETED,
				),
				state.And(
					state.StatusEQ(state.StatusPENDING),
					state.ResolvedTickCount(24),
				),
			),
			state.TimestampLTE(boundary),
		).
			WithInstrument().
			Order(state.ByTimestamp()).
			ManyForUpdateSkipLocked(ctx, candleBackfillClaimLimit)
		if e != nil {
			return e
		}

		for _, row := range rows {
			originalStatus := row.Status

			var newPrev state.PreviousStatus
			var newStatus state.Status
			var layer candleBackfillLayer

			switch originalStatus {
			case state.StatusPENDING:
				// Layer 1 — aggregation; ResolvedTickCount=24 guaranteed by query filter.
				newPrev, newStatus, layer = state.PreviousStatusPENDING, state.StatusPROCESSED, candleLayerAggregate

			case state.StatusPROCESSED:
				// Zombie: resolved at claim time, no goroutine dispatched.
				newPrev, newStatus, layer = state.PreviousStatusPROCESSED, state.StatusPENDING, candleLayerNone

			case state.StatusFAILED:
				if row.RetryCount > candleMaxRetryCount {
					newPrev, newStatus, layer = state.PreviousStatusFAILED, state.StatusABANDONED, candleLayerNone
				} else {
					newPrev, newStatus, layer = state.PreviousStatusFAILED, state.StatusPROCESSED, candleLayerReset
				}

			case state.StatusBROKEN:
				if row.RetryCount > candleMaxRetryCount {
					newPrev, newStatus, layer = state.PreviousStatusBROKEN, state.StatusABANDONED, candleLayerNone
				} else {
					newPrev, newStatus, layer = state.PreviousStatusBROKEN, state.StatusPROCESSED, candleLayerReset
				}

			case state.StatusCOMPLETED:
				// Layer 3 — orphaned COMPLETED: upload succeeded but worker crashed before validation.
				newPrev, newStatus, layer = state.PreviousStatusCOMPLETED, state.StatusPROCESSED, candleLayerValidation

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

			routed = append(routed, candleRoutedRow{row: updated, layer: layer})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return routed, nil
}

// dispatchCandleBackfillRow runs the handler chosen at master-claim time.
// Rows with candleLayerNone were fully resolved inside the claim transaction.
func dispatchCandleBackfillRow(ctx context.Context, d *dal.DAL, item candleRoutedRow) {
	switch item.layer {
	case candleLayerAggregate:
		executeCandleAggregation(ctx, d, item.row)
	case candleLayerReset:
		executeCandleRetryReset(ctx, d, item.row)
	case candleLayerValidation:
		executeCandleValidation(ctx, d, item.row)
	case candleLayerNone:
		// Already fully resolved inside the claim transaction.
	}
}

// executeCandleValidation handles Layer 3 of Candle Backfill: rows that completed
// the upload (→COMPLETED) but whose worker crashed before the instant read-back
// and validation pass. It re-runs validation and promotes to CONFIRMED or BROKEN.
func executeCandleValidation(ctx context.Context, d *dal.DAL, row *ent.State) {
	ctx, span := candleTracer.Start(ctx, "candles/backfill-validation",
		trace.WithAttributes(
			attribute.String("state.id", row.ID.String()),
			attribute.String("candle.date", row.Timestamp.UTC().Format("2006-01-02")),
		),
	)
	defer span.End()

	if row.Edges.Instrument == nil {
		span.RecordError(fmt.Errorf("candle validation: instrument not loaded for %s", row.ID))
		updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusBROKEN, "candle validation: instrument not loaded")
		return
	}

	stored, err := readCandleParquetFromR2(ctx, row)
	if err != nil {
		span.RecordError(err)
		log.Printf("candle validation %s: read-back (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusBROKEN, "read-back candle parquet failed")
		return
	}

	if err := validateCandleParquet(ctx, row, stored); err != nil {
		span.RecordError(err)
		log.Printf("candle validation %s: validate (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusBROKEN, "validate candle parquet failed")
		return
	}

	updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusCONFIRMED, "set candle CONFIRMED")
}

// resetCandleAbandonedRows is Phase C of Candle Backfill: last-resort reset run
// only when the master bulk-claim yielded 0 actionable rows. It iterates ABANDONED
// CANDLE rows outside the Zona Eksklusif (Timestamp <= boundary) and resets each
// to PENDING with RetryCount=0, giving them a fresh aggregation budget.
func resetCandleAbandonedRows(ctx context.Context, d *dal.DAL, span trace.Span, boundary time.Time) {
	for ctx.Err() == nil {
		err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			row, err := candleStateRows(tx.State.Query(),
				state.StatusEQ(state.StatusABANDONED),
				state.TimestampLTE(boundary),
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
			log.Printf("candle abandoned reset (traceID=%s): %v", span.SpanContext().TraceID(), err)
			break
		}
	}
}

// executeCandleRetryReset applies the Backfill Reset rule for a claimed CANDLE row:
//   - PROCESSED → PENDING + AddRetryCount(+1), or ABANDONED at ≥ 5
//   - FAILED / BROKEN → PENDING + AddRetryCount(+1), or ABANDONED at ≥ 5
func executeCandleRetryReset(ctx context.Context, d *dal.DAL, row *ent.State) {
	if row.PreviousStatus == nil {
		return
	}
	switch *row.PreviousStatus {
	case state.PreviousStatusPROCESSED:
		handleRetryReset(ctx, d, row)

	case state.PreviousStatusFAILED, state.PreviousStatusBROKEN:
		// Increment RetryCount first, then decide PENDING vs. ABANDONED.
		handleRetryReset(ctx, d, row)
	}
}
