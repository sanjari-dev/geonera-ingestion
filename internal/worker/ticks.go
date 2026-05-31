package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
)

var tickTracer = otel.Tracer("worker/ticks")

// RunTickParentHandler is the single entry point for all tick-ingestion work.
// It acquires LockIDTick via the direct connection so that Regular and Backfill
// modes can never run concurrently, then forks the appropriate child goroutines.
//
// mode must be either "REGULAR" or "BACKFILL".
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

	var locked bool
	err = tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", dal.LockIDTick).Scan(&locked)
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
				var alive bool
				err := tx.QueryRowContext(hbCtx, "SELECT true").Scan(&alive)
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
	defer recoverGoroutine(ctx, "runT0Phase")

	// TODO: claim PENDING tick rows for the current hour, download BI5 from
	// Dukascopy (bounded semaphore ≤ 12 goroutines), convert to Parquet,
	// upload to R2, update status → COMPLETED / FAILED / NOT_FOUND.
	log.Println("T-0: not yet implemented")
}

// ── T-1: previous hour recovery ───────────────────────────────────────────────

func runT1Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runT1Phase")

	// TODO: claim PENDING / PROCESSED / FAILED / NOT_FOUND rows for T-1,
	// reset PROCESSED → PENDING, retry FAILED (increment RetryCount → ABANDONED
	// at ≥ 5), handle NOT_FOUND streak logic and Zero-Row Parquet upload.
	log.Println("T-1: not yet implemented")
}

// ── T-2: two-hours-ago physical validation ────────────────────────────────────

func runT2Phase(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runT2Phase")

	// TODO: claim COMPLETED rows for T-2, read Parquet from R2, run physical
	// validation (magic number, schema match, boundary check), promote to
	// CONFIRMED or demote to BROKEN, insert SyncTask transactionally.
	log.Println("T-2: not yet implemented")
}

// ── Backfill groups ───────────────────────────────────────────────────────────

func runBackfillIngestionGroup(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runBackfillIngestionGroup")

	// TODO: claim historical PENDING rows, run standard ETL cycle.
	log.Println("backfill ingestion: not yet implemented")
}

func runBackfillResetGroup(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runBackfillResetGroup")

	// TODO: claim PROCESSED / FAILED / BROKEN rows, reset or ABANDON at retry ≥ 5.
	log.Println("backfill reset: not yet implemented")
}

func runBackfillValidationGroup(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runBackfillValidationGroup")

	// TODO: validate COMPLETED rows and re-check NOT_FOUND rows via API
	// (bounded semaphore ≤ 12), promote or demote accordingly.
	log.Println("backfill validation: not yet implemented")
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
