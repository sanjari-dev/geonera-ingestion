package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
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
		log.Printf("candles %s: acquire lock: %v", mode, err)
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

// runCandleRegular seeds today's CANDLE row and aggregates yesterday's ticks
// when ResolvedTickCount = 24 (all 24 hourly TICK rows are CONFIRMED).
func runCandleRegular(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runCandleRegular")

	// TODO:
	// 1. Upsert CANDLE row for today 00:00:00 UTC with status PENDING.
	// 2. Claim PENDING CANDLE rows where ResolvedTickCount = 24.
	// 3. Stream-aggregate 24 hourly Parquet files from R2 (O(1) memory).
	// 4. Build one daily Candles Parquet (all 19 timeframes).
	// 5. Upload to R2, validate, promote to CONFIRMED.
	log.Println("candle regular: not yet implemented")
}

// runCandleBackfill resets stuck/failed CANDLE rows and executes overdue
// aggregations for any PENDING row where ResolvedTickCount = 24.
func runCandleBackfill(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runCandleBackfill")

	// TODO:
	// 1. Claim FAILED / BROKEN / PROCESSED rows → reset to PENDING,
	//    increment RetryCount atomically; ABANDON at ≥ 5.
	// 2. Claim historical PENDING rows where ResolvedTickCount = 24,
	//    run the same stream-aggregation pipeline as regular.
	log.Println("candle backfill: not yet implemented")
}
