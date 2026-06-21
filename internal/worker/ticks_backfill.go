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
	// backfillLayerRetryReset: FAILED/BROKEN → PROCESSED, apply the handleRetryReset
	// (RetryCount+1 / NotFoundStreak-1 → PENDING; no ABANDONED cap).
	backfillLayerRetryReset
	// backfillLayerT2Action: COMPLETED or NOT_FOUND (streak >= threshold) → PROCESSED,
	// run the same re-download and cross-validation logic as T-2 (executeT2Action):
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

	boundary := backfillTimeBoundary()
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runBackfillMasterLoop", "boundary": boundary.Format(time.RFC3339)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runBackfillMasterLoop").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{"boundary": boundary.Format(time.RFC3339), "claim_limit": backfillMasterClaimLimit}).Info("ticks/backfill: start")

	// Claim up to backfillMasterClaimLimit rows once and dispatch in parallel.
	// ABANDONED rows are sorted last, so they are only reached when no other
	// actionable rows exist within the T-3 boundary.
	ilogger.T(ctx).WithField("fn", "runBackfillMasterLoop").Trace("before call: runBackfillMasterClaim")
	runBackfillMasterClaim(ctx, d)
	ilogger.T(ctx).WithField("fn", "runBackfillMasterLoop").Trace("after call: runBackfillMasterClaim")

	ilogger.T(ctx).WithField("boundary", boundary.Format(time.RFC3339)).Info("ticks/backfill: done")
}

// runBackfillMasterClaim implements the "Master Bulk-Claim" strategy: claim up
// to backfillMasterClaimLimit rows in a single FOR UPDATE SKIP LOCKED transaction
// (the oldest Timestamp first, Timestamp <= T-3), apply the per-row routing decision,
// dispatch all rows in parallel goroutines, and wait for them to finish.
// Each backfill trigger processes exactly one batch of up to 120 rows — no loop.
// The only real throttles are the download rate-gate/semaphore (12 concurrent)
// and the convert/upload semaphore (runtime.NumCPU()).
func runBackfillMasterClaim(ctx context.Context, d *dal.DAL) {
	ilogger.T(ctx).WithField("fn", "runBackfillMasterClaim").Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runBackfillMasterClaim").Trace("fn_exit")

	boundary := backfillTimeBoundary()

	ilogger.T(ctx).WithFields(logrus.Fields{
		"boundary": boundary.Format(time.RFC3339), "claim_limit": backfillMasterClaimLimit,
	}).Debug("ticks/backfill: claim params")
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runBackfillMasterClaim", "boundary": boundary.Format(time.RFC3339)}).Trace("before call: claimBackfillMasterBatch")
	batch, err := claimBackfillMasterBatch(ctx, d, boundary)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runBackfillMasterClaim", "err": err != nil}).Trace("after call: claimBackfillMasterBatch")
	if err != nil {
		if ent.IsNotFound(err) {
			ilogger.T(ctx).WithField("boundary", boundary.Format(time.RFC3339)).Info("ticks/backfill: no rows to process")
		} else {
			ilogger.T(ctx).WithError(err).WithField("boundary", boundary.Format(time.RFC3339)).Error("ticks/backfill: master-claim error")
		}
		return
	}
	if len(batch) == 0 {
		ilogger.T(ctx).WithField("fn", "runBackfillMasterClaim").Trace("branch: empty_batch")
		ilogger.T(ctx).WithField("boundary", boundary.Format(time.RFC3339)).Info("ticks/backfill: empty batch")
		return
	}

	// Count per layer for a single-line summary before dispatching.
	layerCounts := map[backfillLayer]int{}
	for _, item := range batch {
		layerCounts[item.layer]++
	}
	ilogger.T(ctx).WithFields(logrus.Fields{
		"batch_size": len(batch),
		"ingestion":  layerCounts[backfillLayerIngestion],
		"retry":      layerCounts[backfillLayerRetryReset],
		"t2":         layerCounts[backfillLayerT2Action],
		"nf_recheck": layerCounts[backfillLayerNotFoundRecheck],
		"none":       layerCounts[backfillLayerNone],
	}).Info("ticks/backfill: claimed rows")

	var batchWg sync.WaitGroup
	for _, item := range batch {
		batchWg.Add(1)
		go func(it backfillRoutedRow) {
			defer recoverGoroutine(ctx, "ticks/backfill-master-row")
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(it.row), "layer": it.layer}).Trace("goroutine: backfill-master-row start")
			dispatchBackfillRoutedRow(ctx, d, it, batchWg.Done)
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(it.row)}).Trace("goroutine: backfill-master-row done")
		}(item)
	}
	ilogger.T(ctx).WithField("fn", "runBackfillMasterClaim").Trace("before batchWg.Wait")
	batchWg.Wait()
	ilogger.T(ctx).WithField("fn", "runBackfillMasterClaim").Trace("after batchWg.Wait")
	ilogger.T(ctx).WithField("rows", len(batch)).Info("ticks/backfill: batch complete")
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
// runNotFoundRecheckLoop for the Backfill path.
func claimBackfillMasterBatch(ctx context.Context, d *dal.DAL, boundary time.Time) ([]backfillRoutedRow, error) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "claimBackfillMasterBatch", "boundary": boundary.Format(time.RFC3339)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "claimBackfillMasterBatch").Trace("fn_exit")

	var routed []backfillRoutedRow
	err := d.Execute(ctx, func(tx *ent.Tx) error {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "limit": backfillMasterClaimLimit}).Trace("before query")
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
				state.StatusCOMPLETED,
				state.StatusNOT_FOUND,
				state.StatusBROKEN,
				state.StatusFAILED,
				state.StatusPROCESSED,
				state.StatusPENDING,
				// ABANDONED is intentionally omitted — the ELSE clause places it last.
			)).
			Order(state.ByTimestamp()).
			ManyForUpdateSkipLocked(ctx, backfillMasterClaimLimit)
		if e != nil {
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "err": true}).Trace("after query")
			return e
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "count": len(rows)}).Trace("after query")
		ilogger.T(ctx).WithFields(logrus.Fields{
			"boundary": boundary.Format(time.RFC3339), "rows_returned": len(rows), "claim_limit": backfillMasterClaimLimit,
		}).Debug("ticks/backfill: query result")
		if len(rows) == 0 {
			return &ent.NotFoundError{}
		}

		for _, row := range rows {
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "status": row.Status, "ts": row.Timestamp.Format(time.RFC3339)}).Trace("loop: backfill claim row start")
			preclaimPrev := row.PreviousStatus
			originalStatus := row.Status

			var newPrev state.PreviousStatus
			var newStatus state.Status
			var layer backfillLayer

			switch originalStatus {
			case state.StatusPENDING:
				// Layer A — Ingesti Utama: download & process the lagging row.
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "status": "PENDING"}).Trace("branch: claim_routing_ingestion")
				newPrev, newStatus, layer = state.PreviousStatusPENDING, state.StatusPROCESSED, backfillLayerIngestion

			case state.StatusPROCESSED:
				// Zombie row from a previous in-flight attempt. Per Mapping
				// State.md §2.B, PROCESSED zombies go straight back to PENDING
				// with no counter-mutation — resolved entirely at claim time.
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "status": "PROCESSED"}).Trace("branch: claim_routing_zombie")
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "ts": row.Timestamp.Format(time.RFC3339)}).Info("ticks/backfill: claim zombie → PENDING")
				newPrev, newStatus, layer = state.PreviousStatusPROCESSED, state.StatusPENDING, backfillLayerNone

			case state.StatusFAILED:
				if row.RetryCount > 5 {
					// Retry budget exhausted — retire to ABANDONED at claim time.
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "retry_count": row.RetryCount}).Trace("branch: claim_routing_failed_abandoned")
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "ts": row.Timestamp.Format(time.RFC3339), "retry_count": row.RetryCount}).Info("ticks/backfill: claim FAILED→ABANDONED (budget exhausted)")
					newPrev, newStatus, layer = state.PreviousStatusFAILED, state.StatusABANDONED, backfillLayerNone
				} else {
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "retry_count": row.RetryCount}).Trace("branch: claim_routing_failed_retry")
					newPrev, newStatus, layer = state.PreviousStatusFAILED, state.StatusPROCESSED, backfillLayerRetryReset
				}

			case state.StatusBROKEN:
				if row.RetryCount > 5 {
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "retry_count": row.RetryCount}).Trace("branch: claim_routing_broken_abandoned")
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "ts": row.Timestamp.Format(time.RFC3339), "retry_count": row.RetryCount}).Info("ticks/backfill: claim BROKEN→ABANDONED (budget exhausted)")
					newPrev, newStatus, layer = state.PreviousStatusBROKEN, state.StatusABANDONED, backfillLayerNone
				} else {
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "retry_count": row.RetryCount}).Trace("branch: claim_routing_broken_retry")
					newPrev, newStatus, layer = state.PreviousStatusBROKEN, state.StatusPROCESSED, backfillLayerRetryReset
				}

			case state.StatusCOMPLETED:
				// Layer C — T-2 style: re-download + cross-validate, same as T-2.
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row)}).Trace("branch: claim_routing_completed_t2")
				newPrev, newStatus, layer = state.PreviousStatusCOMPLETED, state.StatusPROCESSED, backfillLayerT2Action

			case state.StatusNOT_FOUND:
				if row.NotFoundStreak >= notFoundThreshold {
					// Layer C — T-2 style: re-download + cross-validate, same as T-2.
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "streak": row.NotFoundStreak}).Trace("branch: claim_routing_not_found_t2")
					newPrev, newStatus, layer = state.PreviousStatusNOT_FOUND, state.StatusPROCESSED, backfillLayerT2Action
				} else {
					// Layer B: streak below a threshold — re-download like T-1.
					// 2xx → PENDING, 404 → Zero-Row + NOT_FOUND, error → FAILED.
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "streak": row.NotFoundStreak}).Trace("branch: claim_routing_not_found_recheck")
					newPrev, newStatus, layer = state.PreviousStatusNOT_FOUND, state.StatusPROCESSED, backfillLayerNotFoundRecheck
				}

			case state.StatusABANDONED:
				// ABANDONED rows only reach this branch after all other statuses
				// are exhausted (status-priority sort puts them last). Reset with
				// RetryCount=0 so they get a clean ingestion cycle.
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row)}).Trace("branch: claim_routing_abandoned_reset")
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "ts": row.Timestamp.Format(time.RFC3339)}).Info("ticks/backfill: claim ABANDONED→PENDING (retry count reset)")
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "instrument": instrName(row)}).Trace("before query")
				updated, ue := tx.State.UpdateOneID(row.ID).
					SetPreviousStatus(state.PreviousStatusABANDONED).
					SetStatus(state.StatusPENDING).
					SetRetryCount(0).
					SetUpdatedAt(time.Now().UTC()).
					Save(ctx)
				if ue != nil {
					ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
					return ue
				}
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false}).Trace("after query")
				updated.Edges = row.Edges
				routed = append(routed, backfillRoutedRow{
					row:          updated,
					preclaimPrev: preclaimPrev,
					layer:        backfillLayerNone,
				})
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(updated), "layer": backfillLayerNone, "new_status": state.StatusPENDING}).Trace("loop: backfill claim row end (abandoned reset)")
				continue

			default:
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "status": row.Status}).Trace("loop: backfill claim row skip (default)")
				continue
			}

			updated, ue := applyStateClaimUpdate(ctx, tx, row, newPrev, newStatus, layer)
			if ue != nil {
				return ue
			}

			routed = append(routed, backfillRoutedRow{
				row:          updated,
				preclaimPrev: preclaimPrev,
				layer:        layer,
			})
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(updated), "layer": layer, "new_status": newStatus}).Trace("loop: backfill claim row end")
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
func dispatchBackfillRoutedRow(ctx context.Context, d *dal.DAL, item backfillRoutedRow, done func()) {
	inst := instrName(item.row)
	ts := item.row.Timestamp.Format(time.RFC3339)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "dispatchBackfillRoutedRow", "instrument": inst, "layer": item.layer}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "dispatchBackfillRoutedRow").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{
		"state_id": item.row.ID, "ts": ts,
		"retry_count": item.row.RetryCount, "streak": item.row.NotFoundStreak,
		"prev_status": prevStatusStr(item.preclaimPrev), "layer": item.layer,
	}).Debug("ticks/backfill: dispatch row vars")
	switch item.layer {
	case backfillLayerIngestion:
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Trace("branch: dispatch_ingestion")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Info("ticks/backfill: dispatch ingestion")
		ilogger.T(ctx).WithField("fn", "dispatchBackfillRoutedRow").Trace("before call: executeIngestionDownload")
		executeIngestionDownload(ctx, d, item.row, handleNotFoundIncrement, false, done)
		ilogger.T(ctx).WithField("fn", "dispatchBackfillRoutedRow").Trace("after call: executeIngestionDownload")
	case backfillLayerRetryReset:
		defer done()
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "retry_count": item.row.RetryCount}).Trace("branch: dispatch_retry_reset")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "retry_count": item.row.RetryCount}).Info("ticks/backfill: dispatch retry_reset")
		ilogger.T(ctx).WithField("fn", "dispatchBackfillRoutedRow").Trace("before call: handleRetryReset")
		handleRetryReset(ctx, d, item.row)
		ilogger.T(ctx).WithField("fn", "dispatchBackfillRoutedRow").Trace("after call: handleRetryReset")
	case backfillLayerT2Action:
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "prev_status": prevStatusStr(item.preclaimPrev), "streak": item.row.NotFoundStreak}).Trace("branch: dispatch_t2_action")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "prev_status": prevStatusStr(item.preclaimPrev), "streak": item.row.NotFoundStreak}).Info("ticks/backfill: dispatch t2_action")
		ilogger.T(ctx).WithField("fn", "dispatchBackfillRoutedRow").Trace("before call: executeT2Download")
		executeT2Download(ctx, d, item.row, item.preclaimPrev, false, done)
		ilogger.T(ctx).WithField("fn", "dispatchBackfillRoutedRow").Trace("after call: executeT2Download")
	case backfillLayerNotFoundRecheck:
		defer done()
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "streak": item.row.NotFoundStreak}).Trace("branch: dispatch_nf_recheck")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "streak": item.row.NotFoundStreak}).Info("ticks/backfill: dispatch nf_recheck")
		ilogger.T(ctx).WithField("fn", "dispatchBackfillRoutedRow").Trace("before call: executeNotFoundRecheck")
		executeNotFoundRecheck(ctx, d, item.row, false)
		ilogger.T(ctx).WithField("fn", "dispatchBackfillRoutedRow").Trace("after call: executeNotFoundRecheck")
	case backfillLayerNone:
		defer done()
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Trace("branch: dispatch_none_already_resolved")
		// Already fully resolved inside the claim transaction (zombie/ABANDONED reset).
	}
}
