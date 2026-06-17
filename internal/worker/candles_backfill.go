package worker

import (
	"context"
	"log"
	"sync"
	"time"

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

	boundary := backfillCandleBoundary()
	log.Printf("candle/backfill: start boundary=%s claim_limit=%d", boundary.Format(time.DateOnly), candleBackfillClaimLimit)

	batch, err := claimCandleBackfillBatch(ctx, d, boundary)
	if err != nil {
		log.Printf("candle backfill: master claim: %v", err)
		return
	}

	if len(batch) == 0 {
		log.Printf("candle/backfill: no rows to process boundary=%s", boundary.Format(time.DateOnly))
	} else {
		layerCounts := map[candleBackfillLayer]int{}
		for _, item := range batch {
			layerCounts[item.layer]++
		}
		log.Printf("candle/backfill: claimed %d row(s) aggregate=%d reset=%d validation=%d none=%d",
			len(batch),
			layerCounts[candleLayerAggregate],
			layerCounts[candleLayerReset],
			layerCounts[candleLayerValidation],
			layerCounts[candleLayerNone],
		)
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
		resetCandleAbandonedRows(ctx, d, boundary)
	}
	log.Printf("candle/backfill: done boundary=%s rows=%d", boundary.Format(time.DateOnly), len(batch))
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
	err := d.Execute(ctx, func(tx *ent.Tx) error {
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
				log.Printf("candle/backfill: claim zombie instrument=%s date=%s → PENDING",
					instrName(row), row.Timestamp.Format(time.DateOnly))
				newPrev, newStatus, layer = state.PreviousStatusPROCESSED, state.StatusPENDING, candleLayerNone

			case state.StatusFAILED:
				if row.RetryCount > candleMaxRetryCount {
					log.Printf("candle/backfill: claim FAILED→ABANDONED instrument=%s date=%s retry_count=%d",
						instrName(row), row.Timestamp.Format(time.DateOnly), row.RetryCount)
					newPrev, newStatus, layer = state.PreviousStatusFAILED, state.StatusABANDONED, candleLayerNone
				} else {
					newPrev, newStatus, layer = state.PreviousStatusFAILED, state.StatusPROCESSED, candleLayerReset
				}

			case state.StatusBROKEN:
				if row.RetryCount > candleMaxRetryCount {
					log.Printf("candle/backfill: claim BROKEN→ABANDONED instrument=%s date=%s retry_count=%d",
						instrName(row), row.Timestamp.Format(time.DateOnly), row.RetryCount)
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
	inst := instrName(item.row)
	date := item.row.Timestamp.Format(time.DateOnly)
	switch item.layer {
	case candleLayerAggregate:
		log.Printf("candle/backfill: dispatch aggregate instrument=%s date=%s", inst, date)
		executeCandleAggregation(ctx, d, item.row)
	case candleLayerReset:
		log.Printf("candle/backfill: dispatch reset instrument=%s date=%s retry_count=%d", inst, date, item.row.RetryCount)
		executeCandleRetryReset(ctx, d, item.row)
	case candleLayerValidation:
		log.Printf("candle/backfill: dispatch validation instrument=%s date=%s", inst, date)
		executeCandleValidation(ctx, d, item.row)
	case candleLayerNone:
		// Already fully resolved inside the claim transaction.
	}
}

// executeCandleValidation handles Layer 3 of Candle Backfill: rows that completed
// the upload (→COMPLETED) but whose worker crashed before the instant read-back
// and validation pass. It re-runs validation and promotes to CONFIRMED or BROKEN.
func executeCandleValidation(ctx context.Context, d *dal.DAL, row *ent.State) {
	if row.Edges.Instrument == nil {
		updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusBROKEN, "candle validation: instrument not loaded")
		return
	}

	stored, err := readCandleParquetFromR2(ctx, row)
	if err != nil {
		log.Printf("candle validation %s: read-back: %v", row.ID, err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusBROKEN, "read-back candle parquet failed")
		return
	}

	if err := validateCandleParquet(ctx, row, stored); err != nil {
		log.Printf("candle validation %s: validate: %v", row.ID, err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusBROKEN, "validate candle parquet failed")
		return
	}

	updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusCONFIRMED, "set candle CONFIRMED")
}

// resetCandleAbandonedRows is Phase C of Candle Backfill: last-resort reset run
// only when the master bulk-claim yielded 0 actionable rows. It iterates ABANDONED
// CANDLE rows outside the Zona Eksklusif (Timestamp <= boundary) and resets each
// to PENDING with RetryCount=0, giving them a fresh aggregation budget.
func resetCandleAbandonedRows(ctx context.Context, d *dal.DAL, boundary time.Time) {
	for ctx.Err() == nil {
		err := d.Execute(ctx, func(tx *ent.Tx) error {
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
			log.Printf("candle abandoned reset: %v", err)
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

