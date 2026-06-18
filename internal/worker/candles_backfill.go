package worker

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	ilogger "github.com/sanjari-dev/geonera-ingestion/internal/logger"
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
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runCandleBackfill", "boundary": boundary.Format(time.DateOnly)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runCandleBackfill").Trace("fn_exit")

	logrus.WithFields(logrus.Fields{"boundary": boundary.Format(time.DateOnly), "claim_limit": candleBackfillClaimLimit}).Info("candle/backfill: start")

	ilogger.T(ctx).WithField("fn", "runCandleBackfill").Trace("before call: claimCandleBackfillBatch")
	batch, err := claimCandleBackfillBatch(ctx, d, boundary)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runCandleBackfill", "batch_size": len(batch), "err": err != nil}).Trace("after call: claimCandleBackfillBatch")
	if err != nil {
		logrus.WithError(err).Error("candle backfill: master claim")
		return
	}

	if len(batch) == 0 {
		ilogger.T(ctx).WithField("fn", "runCandleBackfill").Trace("branch: empty_batch")
		logrus.WithField("boundary", boundary.Format(time.DateOnly)).Info("candle/backfill: no rows to process")
	} else {
		layerCounts := map[candleBackfillLayer]int{}
		for _, item := range batch {
			layerCounts[item.layer]++
		}
		logrus.WithFields(logrus.Fields{
			"batch_size":  len(batch),
			"aggregate":   layerCounts[candleLayerAggregate],
			"reset":       layerCounts[candleLayerReset],
			"validation":  layerCounts[candleLayerValidation],
			"none":        layerCounts[candleLayerNone],
		}).Info("candle/backfill: claimed rows")
	}

	var batchWg sync.WaitGroup
	for _, item := range batch {
		batchWg.Add(1)
		go func(it candleRoutedRow) {
			defer batchWg.Done()
			defer recoverGoroutine(ctx, "candles/backfill-row")
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(it.row), "layer": it.layer}).Trace("goroutine: candle backfill-row start")
			dispatchCandleBackfillRow(ctx, d, it)
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(it.row)}).Trace("goroutine: candle backfill-row done")
		}(item)
	}
	ilogger.T(ctx).WithField("fn", "runCandleBackfill").Trace("before batchWg.Wait")
	batchWg.Wait()
	ilogger.T(ctx).WithField("fn", "runCandleBackfill").Trace("after batchWg.Wait")

	// Phase C: last-resort ABANDONED reset — only when no actionable rows remain.
	if len(batch) == 0 {
		ilogger.T(ctx).WithField("fn", "runCandleBackfill").Trace("before call: resetCandleAbandonedRows")
		resetCandleAbandonedRows(ctx, d, boundary)
		ilogger.T(ctx).WithField("fn", "runCandleBackfill").Trace("after call: resetCandleAbandonedRows")
	}
	logrus.WithFields(logrus.Fields{"boundary": boundary.Format(time.DateOnly), "rows": len(batch)}).Info("candle/backfill: done")
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
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "claimCandleBackfillBatch", "boundary": boundary.Format(time.DateOnly)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "claimCandleBackfillBatch").Trace("fn_exit")

	var routed []candleRoutedRow
	err := d.Execute(ctx, func(tx *ent.Tx) error {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "limit": candleBackfillClaimLimit}).Trace("before query")
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
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "err": true}).Trace("after query")
			return e
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "count": len(rows)}).Trace("after query")

		for _, row := range rows {
			originalStatus := row.Status

			var newPrev state.PreviousStatus
			var newStatus state.Status
			var layer candleBackfillLayer

			switch originalStatus {
			case state.StatusPENDING:
				// Layer 1 — aggregation; ResolvedTickCount=24 guaranteed by query filter.
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row)}).Trace("branch: candle_claim_routing_pending_aggregate")
				newPrev, newStatus, layer = state.PreviousStatusPENDING, state.StatusPROCESSED, candleLayerAggregate

			case state.StatusPROCESSED:
				// Zombie: resolved at claim time, no goroutine dispatched.
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row)}).Trace("branch: candle_claim_routing_zombie")
				logrus.WithFields(logrus.Fields{"instrument": instrName(row), "date": row.Timestamp.Format(time.DateOnly)}).Info("candle/backfill: claim zombie → PENDING")
				newPrev, newStatus, layer = state.PreviousStatusPROCESSED, state.StatusPENDING, candleLayerNone

			case state.StatusFAILED:
				if row.RetryCount > candleMaxRetryCount {
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "retry_count": row.RetryCount}).Trace("branch: candle_claim_routing_failed_abandoned")
					logrus.WithFields(logrus.Fields{"instrument": instrName(row), "date": row.Timestamp.Format(time.DateOnly), "retry_count": row.RetryCount}).Info("candle/backfill: claim FAILED→ABANDONED")
					newPrev, newStatus, layer = state.PreviousStatusFAILED, state.StatusABANDONED, candleLayerNone
				} else {
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "retry_count": row.RetryCount}).Trace("branch: candle_claim_routing_failed_reset")
					newPrev, newStatus, layer = state.PreviousStatusFAILED, state.StatusPROCESSED, candleLayerReset
				}

			case state.StatusBROKEN:
				if row.RetryCount > candleMaxRetryCount {
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "retry_count": row.RetryCount}).Trace("branch: candle_claim_routing_broken_abandoned")
					logrus.WithFields(logrus.Fields{"instrument": instrName(row), "date": row.Timestamp.Format(time.DateOnly), "retry_count": row.RetryCount}).Info("candle/backfill: claim BROKEN→ABANDONED")
					newPrev, newStatus, layer = state.PreviousStatusBROKEN, state.StatusABANDONED, candleLayerNone
				} else {
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "retry_count": row.RetryCount}).Trace("branch: candle_claim_routing_broken_reset")
					newPrev, newStatus, layer = state.PreviousStatusBROKEN, state.StatusPROCESSED, candleLayerReset
				}

			case state.StatusCOMPLETED:
				// Layer 3 — orphaned COMPLETED: upload succeeded but worker crashed before validation.
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row)}).Trace("branch: candle_claim_routing_completed_validation")
				newPrev, newStatus, layer = state.PreviousStatusCOMPLETED, state.StatusPROCESSED, candleLayerValidation

			default:
				continue
			}

			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "instrument": instrName(row), "new_status": newStatus}).Trace("before query")
			updated, ue := tx.State.UpdateOneID(row.ID).
				SetPreviousStatus(newPrev).
				SetStatus(newStatus).
				SetUpdatedAt(time.Now().UTC()).
				Save(ctx)
			if ue != nil {
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
				return ue
			}
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false, "layer": layer}).Trace("after query")
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
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "dispatchCandleBackfillRow", "instrument": inst, "layer": item.layer}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "dispatchCandleBackfillRow").Trace("fn_exit")

	switch item.layer {
	case candleLayerAggregate:
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "date": date}).Trace("branch: candle_dispatch_aggregate")
		logrus.WithFields(logrus.Fields{"instrument": inst, "date": date}).Info("candle/backfill: dispatch aggregate")
		ilogger.T(ctx).WithField("fn", "dispatchCandleBackfillRow").Trace("before call: executeCandleAggregation")
		executeCandleAggregation(ctx, d, item.row)
		ilogger.T(ctx).WithField("fn", "dispatchCandleBackfillRow").Trace("after call: executeCandleAggregation")
	case candleLayerReset:
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "date": date, "retry_count": item.row.RetryCount}).Trace("branch: candle_dispatch_reset")
		logrus.WithFields(logrus.Fields{"instrument": inst, "date": date, "retry_count": item.row.RetryCount}).Info("candle/backfill: dispatch reset")
		ilogger.T(ctx).WithField("fn", "dispatchCandleBackfillRow").Trace("before call: executeCandleRetryReset")
		executeCandleRetryReset(ctx, d, item.row)
		ilogger.T(ctx).WithField("fn", "dispatchCandleBackfillRow").Trace("after call: executeCandleRetryReset")
	case candleLayerValidation:
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "date": date}).Trace("branch: candle_dispatch_validation")
		logrus.WithFields(logrus.Fields{"instrument": inst, "date": date}).Info("candle/backfill: dispatch validation")
		ilogger.T(ctx).WithField("fn", "dispatchCandleBackfillRow").Trace("before call: executeCandleValidation")
		executeCandleValidation(ctx, d, item.row)
		ilogger.T(ctx).WithField("fn", "dispatchCandleBackfillRow").Trace("after call: executeCandleValidation")
	case candleLayerNone:
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "date": date}).Trace("branch: candle_dispatch_none_resolved")
		// Already fully resolved inside the claim transaction.
	}
}

// executeCandleValidation handles Layer 3 of Candle Backfill: rows that completed
// the upload (→COMPLETED) but whose worker crashed before the instant read-back
// and validation pass. It re-runs validation and promotes to CONFIRMED or BROKEN.
func executeCandleValidation(ctx context.Context, d *dal.DAL, row *ent.State) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleValidation", "instrument": instrName(row)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "executeCandleValidation").Trace("fn_exit")

	if row.Edges.Instrument == nil {
		ilogger.T(ctx).WithField("fn", "executeCandleValidation").Trace("branch: instrument_edge_nil")
		updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusBROKEN, "candle validation: instrument not loaded")
		return
	}

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleValidation", "instrument": instrName(row)}).Trace("before r2 read-back")
	stored, err := readCandleParquetFromR2(ctx, row)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleValidation", "err": err != nil}).Trace("after r2 read-back")
	if err != nil {
		logrus.WithError(err).Errorf("candle validation %s: read-back", row.ID)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusBROKEN, "read-back candle parquet failed")
		return
	}

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleValidation", "bytes": len(stored)}).Trace("before call: validateCandleParquet")
	if err := validateCandleParquet(ctx, row, stored); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleValidation", "err": true}).Trace("after call: validateCandleParquet")
		logrus.WithError(err).Errorf("candle validation %s: validate", row.ID)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusBROKEN, "validate candle parquet failed")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleValidation", "err": false}).Trace("after call: validateCandleParquet")

	updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusCONFIRMED, "set candle CONFIRMED")
}

// resetCandleAbandonedRows is Phase C of Candle Backfill: last-resort reset run
// only when the master bulk-claim yielded 0 actionable rows. It iterates ABANDONED
// CANDLE rows outside the Zona Eksklusif (Timestamp <= boundary) and resets each
// to PENDING with RetryCount=0, giving them a fresh aggregation budget.
func resetCandleAbandonedRows(ctx context.Context, d *dal.DAL, boundary time.Time) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "resetCandleAbandonedRows", "boundary": boundary.Format(time.DateOnly)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "resetCandleAbandonedRows").Trace("fn_exit")

	for ctx.Err() == nil {
		ilogger.T(ctx).WithField("fn", "resetCandleAbandonedRows").Trace("loop: iteration start")

		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.First+UpdateOneID.Save"}).Trace("before query")
		err := d.Execute(ctx, func(tx *ent.Tx) error {
			row, err := candleStateRows(tx.State.Query(),
				state.StatusEQ(state.StatusABANDONED),
				state.TimestampLTE(boundary),
			).Order(state.ByTimestamp()).FirstForUpdateSkipLocked(ctx)
			if err != nil {
				return err
			}
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row)}).Trace("branch: resetting_abandoned_candle")
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
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.First+UpdateOneID.Save", "found": false}).Trace("after query")
				break
			}
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.First+UpdateOneID.Save", "err": true}).Trace("after query")
			logrus.WithError(err).Error("candle abandoned reset")
			break
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.First+UpdateOneID.Save", "err": false}).Trace("after query")

		ilogger.T(ctx).WithField("fn", "resetCandleAbandonedRows").Trace("loop: iteration end")
	}
}

// executeCandleRetryReset applies the Backfill Reset rule for a claimed CANDLE row:
//   - PROCESSED → PENDING + AddRetryCount(+1), or ABANDONED at ≥ 5
//   - FAILED / BROKEN → PENDING + AddRetryCount(+1), or ABANDONED at ≥ 5
func executeCandleRetryReset(ctx context.Context, d *dal.DAL, row *ent.State) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleRetryReset", "instrument": instrName(row), "prev_status": prevStatusStr(row.PreviousStatus)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "executeCandleRetryReset").Trace("fn_exit")

	if row.PreviousStatus == nil {
		ilogger.T(ctx).WithField("fn", "executeCandleRetryReset").Trace("branch: prev_nil_return")
		return
	}
	switch *row.PreviousStatus {
	case state.PreviousStatusPROCESSED:
		ilogger.T(ctx).WithFields(logrus.Fields{"prev": *row.PreviousStatus}).Trace("branch: candle_retry_reset_processed")
		ilogger.T(ctx).WithField("fn", "executeCandleRetryReset").Trace("before call: handleRetryReset")
		handleRetryReset(ctx, d, row)
		ilogger.T(ctx).WithField("fn", "executeCandleRetryReset").Trace("after call: handleRetryReset")

	case state.PreviousStatusFAILED, state.PreviousStatusBROKEN:
		// Increment RetryCount first, then decide PENDING vs. ABANDONED.
		ilogger.T(ctx).WithFields(logrus.Fields{"prev": *row.PreviousStatus}).Trace("branch: candle_retry_reset_failed_or_broken")
		ilogger.T(ctx).WithField("fn", "executeCandleRetryReset").Trace("before call: handleRetryReset")
		handleRetryReset(ctx, d, row)
		ilogger.T(ctx).WithField("fn", "executeCandleRetryReset").Trace("after call: handleRetryReset")
	}
}
