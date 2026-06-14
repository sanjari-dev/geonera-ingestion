package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/instrument"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/candleparquet"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
	"github.com/sanjari-dev/geonera-ingestion/internal/tickparquet"
)

// runCandleRegular implements §4.F:
//  1. Seed: upsert a CANDLE state row for today 00:00:00 UTC (Status=PENDING) for
//     every active instrument. Idempotent — ON CONFLICT DO NOTHING.
//  2. Aggregation: claim PENDING CANDLE rows with ResolvedTickCount=24 and run
//     the full stream-aggregation pipeline by calling executeCandleAggregation.
func runCandleRegular(ctx context.Context, d *dal.DAL, wg *sync.WaitGroup) {
	defer wg.Done()
	defer recoverGoroutine(ctx, "runCandleRegular")

	// Step 1 — Seed today's CANDLE row for all active instruments.
	seedTodayCandleRows(ctx, d)

	// Step 2 — Claim PENDING CANDLE rows with ResolvedTickCount=24 and aggregate.
	runCandleAggregationLoop(ctx, d)
}

// ── Seeding ───────────────────────────────────────────────────────────────────

// seedTodayCandleRows upserts CANDLE PENDING rows for yesterday (D-1) and today (D)
// for every active, unpaused instrument. Seeding yesterday ensures the row exists
// before Stage B's aggregation loop tries to claim it at 05:08 UTC.
// ON CONFLICT DO NOTHING makes both inserts idempotent.
func seedTodayCandleRows(ctx context.Context, d *dal.DAL) {
	today := time.Now().UTC().Truncate(24 * time.Hour)
	yesterday := today.AddDate(0, 0, -1)

	var instruments []*ent.Instrument
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		var e error
		instruments, e = tx.Instrument.Query().
			Where(
				instrument.IsActiveEQ(true),
				instrument.IsPauseEQ(false),
			).
			All(ctx)
		return e
	}); err != nil {
		log.Printf("candle seed: list instruments: %v", err)
		return
	}

	for _, instr := range instruments {
		if ctx.Err() != nil {
			return
		}
		seedOneCandleRow(ctx, d, instr.ID, yesterday)
		seedOneCandleRow(ctx, d, instr.ID, today)
	}
}

// seedOneCandleRow idempotently inserts a CANDLE PENDING row for (instrumentID, today).
func seedOneCandleRow(ctx context.Context, d *dal.DAL, instrumentID uuid.UUID, today time.Time) {
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		return tx.ExecContext(ctx, `
			INSERT INTO ingestion.states (
				id, instrument_id, job_type, timestamp, status,
				is_holiday, resolved_tick_count, retry_count, not_found_streak,
				is_deleted, updated_at
			)
			VALUES ($1, $2, $3, $4, $5, false, 0, 0, 0, false, $6)
			ON CONFLICT (instrument_id, timestamp, job_type) DO NOTHING
		`,
			uuid.New(),
			instrumentID,
			string(state.JobTypeCANDLE),
			today,
			string(state.StatusPENDING),
			time.Now().UTC(),
		)
	}); err != nil {
		log.Printf("candle seed %s %s: %v", instrumentID, today.Format(time.DateOnly), err)
	}
}

// ── Aggregation loop ──────────────────────────────────────────────────────────

// runCandleAggregationLoop claims PENDING CANDLE rows with ResolvedTickCount=24
// and dispatches each to executeCandleAggregation in a goroutine.
// Claims use FOR UPDATE SKIP LOCKED so Regular and Backfill runs safely route work by status.
func runCandleAggregationLoop(ctx context.Context, d *dal.DAL) {
	var loopWg sync.WaitGroup
	for ctx.Err() == nil {
		claimed, err := claimStateAsProcessed(ctx, d, func(q *ent.StateQuery) *ent.StateQuery {
			return candleStateRows(q,
				state.StatusEQ(state.StatusPENDING),
				state.ResolvedTickCount(24),
			).
				WithInstrument().
				Order(state.ByTimestamp())
		})
		if err != nil {
			if ent.IsNotFound(err) {
				break
			}
			log.Printf("candle aggregation claim: %v", err)
			break
		}

		loopWg.Add(1)
		go func(r *ent.State) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "candles/aggregation-row")
			executeCandleAggregation(ctx, d, r)
		}(claimed)
	}
	loopWg.Wait()
}

// ── Aggregation pipeline ──────────────────────────────────────────────────────

// executeCandleAggregation is the main candle aggregation pipeline (§4.F step 2):
//  1. Query the 24 CONFIRMED TICK state rows for this instrument and day.
//  2. Stream each hourly Parquet from R2 into the OHLCV accumulator (O(1) memory).
//     - Skip hours when tickState.IsHoliday == true (Zero-Row tick file).
//     - Handle NoSuchKey: IsDeleted=true → safe skip; IsDeleted=false → BROKEN.
//  3. Build the daily Candles Parquet from the accumulator.
//  4. Upload to R2, read back, validate, promote to CONFIRMED.
//     On any error: PROCESSED → FAILED or BROKEN depending on cause.
func executeCandleAggregation(ctx context.Context, d *dal.DAL, row *ent.State) {
	candleAggSem <- struct{}{}
	defer func() { <-candleAggSem }()

	if row.Edges.Instrument == nil {
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "candle agg: instrument not loaded")
		return
	}
	instrName := row.Edges.Instrument.Name
	dayStart := row.Timestamp.UTC() // CANDLE timestamp is already 00:00:00 UTC

	// Step 1 — Query the 24 CONFIRMED TICK rows for this instrument and day.
	dayEnd := dayStart.Add(24 * time.Hour)

	var tickStates []*ent.State
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
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
		log.Printf("candle agg %s: query tick states: %v", row.ID, err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "query tick states failed")
		return
	}

	// Step 2 — Stream each hourly Parquet from R2 into the candle accumulator.
	acc := newCandleAccumulator(dayStart, instrName)
	broken := false

	for _, tickState := range tickStates {
		if ctx.Err() != nil {
			return
		}

		// §4.F-c: skip Zero-Row files (market holidays).
		if tickState.IsHoliday {
			continue
		}

		data, err := readTickParquetFromR2(ctx, instrName, tickState.Timestamp)
		if err != nil {
			if isNoSuchKeyError(err) {
				// §4.F-d: NoSuchKey handling.
				if tickState.IsDeleted {
					// Pruned row — safe to skip.
					continue
				}
				// BROKEN Storage: file should exist but doesn't.
				log.Printf("candle agg %s: NoSuchKey for tick %s (not deleted)", row.ID, tickState.ID)
				broken = true
				break
			}
			log.Printf("candle agg %s: read tick parquet %s: %v", row.ID, tickState.ID, err)
			updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "read tick parquet failed")
			return
		}

		if err := acc.AccumulateTickParquet(data); err != nil {
			log.Printf("candle agg %s: accumulate tick %s: %v", row.ID, tickState.ID, err)
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
		log.Printf("candle agg %s: build candle parquet: %v", row.ID, err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "build candle parquet failed")
		return
	}

	// Step 4a — Upload to R2.
	if err := uploadCandleParquetToR2(ctx, row, candleData); err != nil {
		log.Printf("candle agg %s: upload candle parquet: %v", row.ID, err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "upload candle parquet failed")
		return
	}
	updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusCOMPLETED, "set candle COMPLETED")

	// Step 4b — Read back and validate.
	stored, err := readCandleParquetFromR2(ctx, row)
	if err != nil {
		log.Printf("candle agg %s: read back candle parquet: %v", row.ID, err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusBROKEN, "read-back candle parquet failed")
		return
	}

	if err := validateCandleParquet(ctx, row, stored); err != nil {
		log.Printf("candle agg %s: validate candle parquet: %v", row.ID, err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusBROKEN, "validate candle parquet failed")
		return
	}

	// Step 4c — Promote to CONFIRMED after readback validation.
	updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusCONFIRMED, "set candle CONFIRMED")
}

// readTickParquetFromR2 fetches the hourly tick Parquet for the specified instrument and timestamp.
// Returns errNoSuchKey if the R2 object is absent.
func readTickParquetFromR2(ctx context.Context, instrument string, ts time.Time) ([]byte, error) {
	if r2Client == nil {
		return nil, errClientsNotInitialized
	}
	key := r2.TickObjectKey(instrument, ts)
	data, err := r2Client.GetObject(ctx, key)
	if errors.Is(err, r2.ErrNotFound) {
		return nil, errNoSuchKey
	}
	if err != nil {
		return nil, fmt.Errorf("readTickParquetFromR2 %s: %w", key, err)
	}
	return data, nil
}

// buildCandleParquet assembles a daily Candles Parquet file (all 19 timeframes)
// from the candle accumulator built by streaming the 24 hourly tick Parquets.
func buildCandleParquet(_ context.Context, _ *ent.State, acc *candleAccumulator) ([]byte, error) {
	var rows []candleparquet.Row

	for _, tf := range candleparquet.All19 {
		buckets := acc.data[tf.Minutes]
		// Collect and sort period keys for deterministic output order.
		keys := make([]int64, 0, len(buckets))
		for k := range buckets {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

		for _, k := range keys {
			b := buckets[k]
			if !b.initialized {
				continue
			}

			volumeWeightedAveragePrice := 0.0
			if b.totalVolume > 0 {
				volumeWeightedAveragePrice = b.weightedMidVolume / b.totalVolume
			}
			avgSpread := 0.0
			if b.tickCount > 0 {
				avgSpread = b.spreadSum / float64(b.tickCount)
			}

			rows = append(rows, candleparquet.Row{
				Timestamp:                  k,
				Instrument:                 acc.instrumentName,
				Timeframe:                  tf.Name,
				Open:                       b.open,
				High:                       b.high,
				Low:                        b.low,
				Close:                      b.close,
				VolumeWeightedAveragePrice: volumeWeightedAveragePrice,
				MinSpread:                  b.minSpread,
				MaxSpread:                  b.maxSpread,
				AvgSpread:                  avgSpread,
				TickCount:                  b.tickCount,
				TotalBidVolume:             b.totalBidVol,
				TotalAskVolume:             b.totalAskVol,
			})
		}
	}
	return candleparquet.Write(rows)
}

// uploadCandleParquetToR2 puts the daily Candle Parquet bytes at the canonical
// R2 object path for this candle row.
func uploadCandleParquetToR2(ctx context.Context, row *ent.State, data []byte) error {
	if r2Client == nil {
		return errClientsNotInitialized
	}
	if row.Edges.Instrument == nil {
		return fmt.Errorf("uploadCandleParquetToR2: instrument not loaded for %s", row.ID)
	}
	key := r2.CandleObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	return r2Client.PutObject(ctx, key, data)
}

// ── Candle accumulator ────────────────────────────────────────────────────────

// periodBucket holds the aggregated candle values for one timeframe period.
type periodBucket struct {
	open, high, low, close float64
	weightedMidVolume      float64
	totalVolume            float64
	spreadSum              float64
	minSpread, maxSpread   float64
	tickCount              int64
	totalBidVol            int64
	totalAskVol            int64
	initialized            bool
}

// candleAccumulator accumulates tick data from the 24 hourly Parquet files into
// candle buckets for all 19 timeframes.  One AccumulateTickParquet call
// per hourly file; O(1) peak memory relative to the number of files.
type candleAccumulator struct {
	dayStart       time.Time
	instrumentName string
	// data[timeframeMinutes][periodStartMicros] → bucket
	data map[int]map[int64]*periodBucket
}

// newCandleAccumulator returns a fresh, empty accumulator for the given day and instrument.
func newCandleAccumulator(dayStart time.Time, instrumentName string) *candleAccumulator {
	data := make(map[int]map[int64]*periodBucket, len(candleparquet.All19))
	for _, tf := range candleparquet.All19 {
		data[tf.Minutes] = make(map[int64]*periodBucket)
	}
	return &candleAccumulator{dayStart: dayStart, instrumentName: instrumentName, data: data}
}

// AccumulateTickParquet parses one hourly tick Parquet file and accumulates
// its rows into candle buckets.  Zero-row (holiday) files are silently skipped.
func (a *candleAccumulator) AccumulateTickParquet(raw []byte) error {
	rows, err := tickparquet.ReadAll(raw)
	if err != nil {
		return fmt.Errorf("candle accumulate: %w", err)
	}
	if len(rows) == 0 {
		return nil // holiday hour
	}

	for _, row := range rows {
		ts := time.UnixMicro(row.Timestamp).UTC()
		mid := (row.Bid + row.Ask) / 2
		spread := row.Ask - row.Bid
		totalVol := float64(row.BidVolume + row.AskVolume)

		for _, tf := range candleparquet.All19 {
			elapsed := ts.Sub(a.dayStart)
			periodIdx := int64(elapsed.Minutes()) / int64(tf.Minutes)
			periodStart := a.dayStart.Add(time.Duration(periodIdx*int64(tf.Minutes)) * time.Minute)
			key := periodStart.UnixMicro()

			b, exists := a.data[tf.Minutes][key]
			if !exists {
				b = &periodBucket{}
				a.data[tf.Minutes][key] = b
			}
			if !b.initialized {
				b.open = mid
				b.high = mid
				b.low = mid
				b.minSpread = spread
				b.maxSpread = spread
				b.initialized = true
			} else {
				if mid > b.high {
					b.high = mid
				}
				if mid < b.low {
					b.low = mid
				}
				if spread < b.minSpread {
					b.minSpread = spread
				}
				if spread > b.maxSpread {
					b.maxSpread = spread
				}
			}
			b.close = mid
			b.weightedMidVolume += mid * totalVol
			b.totalVolume += totalVol
			b.spreadSum += spread
			b.tickCount++
			b.totalBidVol += row.BidVolume
			b.totalAskVol += row.AskVolume
		}
	}
	return nil
}
