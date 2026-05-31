package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
)

var maintenanceTracer = otel.Tracer("worker/maintenance")

// RunMaintenanceHandler runs the Auto-Seeder and Pruning cycle.
// It acquires LockIDMaintenance to prevent concurrent maintenance runs
// (e.g., from multiple scheduler instances).
func RunMaintenanceHandler(ctx context.Context, d *dal.DAL) {
	ctx, span := maintenanceTracer.Start(ctx, "RunMaintenanceHandler")
	defer span.End()

	tx, err := d.AcquireAdvisoryLock(ctx)
	if err != nil {
		span.RecordError(err)
		log.Printf("maintenance: acquire lock: %v", err)
		return
	}
	defer func() { _ = tx.Rollback() }()

	var locked bool
	err = tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", dal.LockIDMaintenance).Scan(&locked)
	if err != nil || !locked {
		span.AddEvent("LockIDMaintenance already held by another instance, skipping")
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
					span.RecordError(fmt.Errorf("maintenance lock heartbeat lost: %w", err))
					cancelETL()
					return
				}
			}
		}
	}()

	runForwardSeeding(lockCtx, d)
	runHistoricalGapFill(lockCtx, d)
	runPruning(lockCtx, d)
}

// runForwardSeeding books future PENDING rows up to 30 days ahead per
// active instrument and job_type (batched to avoid OOM / long table locks).
func runForwardSeeding(ctx context.Context, d *dal.DAL) {
	defer recoverGoroutine(ctx, "runForwardSeeding")

	// TODO:
	// For each active instrument × job_type:
	//   1. Find MAX(timestamp) for that instrument + job_type.
	//   2. INSERT ... ON CONFLICT DO NOTHING up to min(MAX+30d, now()).
	log.Println("forward seeding: not yet implemented")
}

// runHistoricalGapFill detects gaps between start_date and the earliest
// CONFIRMED tick, backfills missing rows, and activates IsPause if stuck.
func runHistoricalGapFill(ctx context.Context, d *dal.DAL) {
	defer recoverGoroutine(ctx, "runHistoricalGapFill")

	// TODO:
	// For each active instrument:
	//   1. Find MIN(timestamp) WHERE job_type=TICK AND status=CONFIRMED.
	//   2. If nil → new instrument, seed forward only (no IsPause).
	//   3. If MIN > start_date → backfill missing rows in batches.
	//   4. Run isBackfillComplete() to decide whether to set IsPause=true.
	//   5. If backfill is complete → clear IsPause.
	log.Println("historical gap fill: not yet implemented")
}

// runPruning soft-deletes rows older than start_date (Mark phase), then
// removes their R2 files, and hard-deletes from the DB (Sweep phase).
func runPruning(ctx context.Context, d *dal.DAL) {
	defer recoverGoroutine(ctx, "runPruning")

	// TODO:
	// Phase 1 — Mark: UPDATE states SET is_deleted=true
	//   WHERE timestamp < instrument.start_date AND is_deleted=false (batched).
	//
	// Phase 2 — Sweep: for each is_deleted=true row:
	//   1. Delete R2 object.
	//   2. ONLY IF R2 responds 200 or NoSuchKey → hard DELETE from Postgres.
	//   3. On R2 timeout/error → leave is_deleted=true for next cycle.
	log.Println("pruning: not yet implemented")
}
