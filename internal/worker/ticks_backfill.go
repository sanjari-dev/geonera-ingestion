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
	log.Printf("ticks/backfill: start boundary=%s claim_limit=%d", boundary.Format(time.RFC3339), backfillMasterClaimLimit)

	// Claim up to backfillMasterClaimLimit rows once and dispatch in parallel.
	// ABANDONED rows are sorted last, so they are only reached when no other
	// actionable rows exist within the T-3 boundary.
	runBackfillMasterClaim(ctx, d)

	log.Printf("ticks/backfill: done boundary=%s", boundary.Format(time.RFC3339))
}

// runBackfillMasterClaim implements the "Master Bulk-Claim" strategy: claim up
// to backfillMasterClaimLimit rows in a single FOR UPDATE SKIP LOCKED transaction
// (the oldest Timestamp first, Timestamp <= T-3), apply the per-row routing decision,
// dispatch all rows in parallel goroutines, and wait for them to finish.
// Each backfill trigger processes exactly one batch of up to 120 rows — no loop.
// The only real throttles are the download rate-gate/semaphore (12 concurrent)
// and the convert/upload semaphore (runtime.NumCPU()).
func runBackfillMasterClaim(ctx context.Context, d *dal.DAL) {
	boundary := backfillTimeBoundary()

	batch, err := claimBackfillMasterBatch(ctx, d, boundary)
	if err != nil {
		if ent.IsNotFound(err) {
			log.Printf("ticks/backfill: no rows to process (boundary=%s)", boundary.Format(time.RFC3339))
		} else {
			log.Printf("ticks/backfill: master-claim error boundary=%s err=%v", boundary.Format(time.RFC3339), err)
		}
		return
	}
	if len(batch) == 0 {
		log.Printf("ticks/backfill: empty batch (boundary=%s)", boundary.Format(time.RFC3339))
		return
	}

	// Count per layer for a single-line summary before dispatching.
	layerCounts := map[backfillLayer]int{}
	for _, item := range batch {
		layerCounts[item.layer]++
	}
	log.Printf("ticks/backfill: claimed %d row(s) ingestion=%d retry=%d t2=%d nf_recheck=%d none=%d",
		len(batch),
		layerCounts[backfillLayerIngestion],
		layerCounts[backfillLayerRetryReset],
		layerCounts[backfillLayerT2Action],
		layerCounts[backfillLayerNotFoundRecheck],
		layerCounts[backfillLayerNone],
	)

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
	log.Printf("ticks/backfill: batch complete rows=%d", len(batch))
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
	var routed []backfillRoutedRow
	err := d.Execute(ctx, func(tx *ent.Tx) error {
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
				log.Printf("ticks/backfill: claim zombie instrument=%s ts=%s → PENDING",
					instrName(row), row.Timestamp.Format(time.RFC3339))
				newPrev, newStatus, layer = state.PreviousStatusPROCESSED, state.StatusPENDING, backfillLayerNone

			case state.StatusFAILED:
				if row.RetryCount > 5 {
					// Retry budget exhausted — retire to ABANDONED at claim time.
					log.Printf("ticks/backfill: claim FAILED→ABANDONED instrument=%s ts=%s retry_count=%d (budget exhausted)",
						instrName(row), row.Timestamp.Format(time.RFC3339), row.RetryCount)
					newPrev, newStatus, layer = state.PreviousStatusFAILED, state.StatusABANDONED, backfillLayerNone
				} else {
					newPrev, newStatus, layer = state.PreviousStatusFAILED, state.StatusPROCESSED, backfillLayerRetryReset
				}

			case state.StatusBROKEN:
				if row.RetryCount > 5 {
					log.Printf("ticks/backfill: claim BROKEN→ABANDONED instrument=%s ts=%s retry_count=%d (budget exhausted)",
						instrName(row), row.Timestamp.Format(time.RFC3339), row.RetryCount)
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
					// Layer B: streak below a threshold — re-download like T-1.
					// 2xx → PENDING, 404 → Zero-Row + NOT_FOUND, error → FAILED.
					newPrev, newStatus, layer = state.PreviousStatusNOT_FOUND, state.StatusPROCESSED, backfillLayerNotFoundRecheck
				}

			case state.StatusABANDONED:
				// ABANDONED rows only reach this branch after all other statuses
				// are exhausted (status-priority sort puts them last). Reset with
				// RetryCount=0 so they get a clean ingestion cycle.
				log.Printf("ticks/backfill: claim ABANDONED→PENDING instrument=%s ts=%s (retry count reset)",
					instrName(row), row.Timestamp.Format(time.RFC3339))
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
	inst := instrName(item.row)
	ts := item.row.Timestamp.Format(time.RFC3339)
	switch item.layer {
	case backfillLayerIngestion:
		log.Printf("ticks/backfill: dispatch ingestion instrument=%s ts=%s", inst, ts)
		executeIngestionETL(ctx, d, item.row, handleNotFoundIncrement, false)
	case backfillLayerRetryReset:
		log.Printf("ticks/backfill: dispatch retry_reset instrument=%s ts=%s retry_count=%d", inst, ts, item.row.RetryCount)
		handleRetryReset(ctx, d, item.row)
	case backfillLayerT2Action:
		log.Printf("ticks/backfill: dispatch t2_action instrument=%s ts=%s prev=%s streak=%d",
			inst, ts, prevStatusStr(item.preclaimPrev), item.row.NotFoundStreak)
		executeT2Action(ctx, d, item.row, item.preclaimPrev, false)
	case backfillLayerNotFoundRecheck:
		log.Printf("ticks/backfill: dispatch nf_recheck instrument=%s ts=%s streak=%d", inst, ts, item.row.NotFoundStreak)
		executeNotFoundRecheck(ctx, d, item.row, false)
	case backfillLayerNone:
		// Already fully resolved inside the claim transaction (zombie/ABANDONED reset).
	}
}
