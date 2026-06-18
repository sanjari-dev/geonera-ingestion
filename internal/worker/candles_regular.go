package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/sirupsen/logrus"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/instrument"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/candleparquet"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	ilogger "github.com/sanjari-dev/geonera-ingestion/internal/logger"
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

	ilogger.T(ctx).WithField("fn", "runCandleRegular").Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runCandleRegular").Trace("fn_exit")

	// Step 1 — Seed today's CANDLE row for all active instruments.
	ilogger.T(ctx).WithField("fn", "runCandleRegular").Trace("before call: seedTodayCandleRows")
	seedTodayCandleRows(ctx, d)
	ilogger.T(ctx).WithField("fn", "runCandleRegular").Trace("after call: seedTodayCandleRows")

	// Step 2 — Claim PENDING CANDLE rows with ResolvedTickCount=24 and aggregate.
	ilogger.T(ctx).WithField("fn", "runCandleRegular").Trace("before call: runCandleAggregationLoop")
	runCandleAggregationLoop(ctx, d)
	ilogger.T(ctx).WithField("fn", "runCandleRegular").Trace("after call: runCandleAggregationLoop")
}

// ── Seeding ───────────────────────────────────────────────────────────────────

// seedTodayCandleRows upserts CANDLE PENDING rows for yesterday (D-1) and today (D)
// for every active, unpaused instrument. Seeding yesterday ensures the row exists
// before Stage B's aggregation loop tries to claim it at 05:08 UTC.
// ON CONFLICT DO NOTHING makes both inserts idempotent.
func seedTodayCandleRows(ctx context.Context, d *dal.DAL) {
	ilogger.T(ctx).WithField("fn", "seedTodayCandleRows").Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "seedTodayCandleRows").Trace("fn_exit")

	today := time.Now().UTC().Truncate(24 * time.Hour)
	yesterday := today.AddDate(0, 0, -1)

	var instruments []*ent.Instrument
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.Query.All"}).Trace("before query")
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
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.Query.All", "err": true}).Trace("after query")
		logrus.WithError(err).Error("candle seed: list instruments")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.Query.All", "count": len(instruments)}).Trace("after query")

	for _, instr := range instruments {
		if ctx.Err() != nil {
			ilogger.T(ctx).WithField("fn", "seedTodayCandleRows").Trace("branch: ctx_canceled")
			return
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "seedTodayCandleRows", "instrument": instr.Name}).Trace("before call: seedOneCandleRow yesterday")
		seedOneCandleRow(ctx, d, instr.ID, yesterday)
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "seedTodayCandleRows", "instrument": instr.Name}).Trace("after call: seedOneCandleRow yesterday")
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "seedTodayCandleRows", "instrument": instr.Name}).Trace("before call: seedOneCandleRow today")
		seedOneCandleRow(ctx, d, instr.ID, today)
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "seedTodayCandleRows", "instrument": instr.Name}).Trace("after call: seedOneCandleRow today")
	}
}

// seedOneCandleRow idempotently inserts a CANDLE PENDING row for (instrumentID, today).
func seedOneCandleRow(ctx context.Context, d *dal.DAL, instrumentID uuid.UUID, today time.Time) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "seedOneCandleRow", "date": today.Format(time.DateOnly)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "seedOneCandleRow").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "date": today.Format(time.DateOnly)}).Trace("before query")
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
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "err": true}).Trace("after query")
		logrus.WithError(err).Errorf("candle seed %s %s", instrumentID, today.Format(time.DateOnly))
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "err": false}).Trace("after query")
}

// ── Aggregation loop ──────────────────────────────────────────────────────────

// runCandleAggregationLoop claims PENDING CANDLE rows with ResolvedTickCount=24
// and dispatches each to executeCandleAggregation in a goroutine.
// Claims use FOR UPDATE SKIP LOCKED so Regular and Backfill runs safely route work by status.
func runCandleAggregationLoop(ctx context.Context, d *dal.DAL) {
	ilogger.T(ctx).WithField("fn", "runCandleAggregationLoop").Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runCandleAggregationLoop").Trace("fn_exit")

	var loopWg sync.WaitGroup
	count := 0
	for ctx.Err() == nil {
		ilogger.T(ctx).WithField("fn", "runCandleAggregationLoop").Trace("loop: iteration start")

		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked"}).Trace("before query")
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
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "found": false}).Trace("after query")
				break
			}
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "err": true}).Trace("after query")
			logrus.WithError(err).Error("candle aggregation claim")
			break
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.ManyForUpdateSkipLocked", "found": true, "instrument": instrName(claimed)}).Trace("after query")

		count++
		logrus.WithFields(logrus.Fields{
			"instrument": instrName(claimed),
			"date":       claimed.Timestamp.Format(time.DateOnly),
			"state_id":   claimed.ID,
		}).Info("candle agg: claimed")

		loopWg.Add(1)
		go func(r *ent.State) {
			defer loopWg.Done()
			defer recoverGoroutine(ctx, "candles/aggregation-row")
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(r), "date": r.Timestamp.Format(time.DateOnly)}).Trace("goroutine: candle aggregation-row start")
			executeCandleAggregation(ctx, d, r)
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(r)}).Trace("goroutine: candle aggregation-row done")
		}(claimed)

		ilogger.T(ctx).WithField("fn", "runCandleAggregationLoop").Trace("loop: iteration end")
	}
	ilogger.T(ctx).WithField("fn", "runCandleAggregationLoop").Trace("before loopWg.Wait")
	loopWg.Wait()
	ilogger.T(ctx).WithField("fn", "runCandleAggregationLoop").Trace("after loopWg.Wait")
	logrus.WithField("rows", count).Info("candle agg: loop complete")
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
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "instrument": instrName(row)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "executeCandleAggregation").Trace("fn_exit")

	ilogger.T(ctx).WithField("fn", "executeCandleAggregation").Trace("before candleAggSem acquire")
	candleAggSem <- struct{}{}
	ilogger.T(ctx).WithField("fn", "executeCandleAggregation").Trace("after candleAggSem acquire")
	defer func() { <-candleAggSem }()

	if row.Edges.Instrument == nil {
		ilogger.T(ctx).WithField("fn", "executeCandleAggregation").Trace("branch: instrument_edge_nil")
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "candle agg: instrument not loaded")
		return
	}
	instrName := row.Edges.Instrument.Name
	dayStart := row.Timestamp.UTC() // CANDLE timestamp is already 00:00:00 UTC
	start := time.Now()
	logrus.WithFields(logrus.Fields{"instrument": instrName, "date": dayStart.Format(time.DateOnly), "state_id": row.ID}).Info("candle agg: start")
	defer func() {
		logrus.WithFields(logrus.Fields{"instrument": instrName, "date": dayStart.Format(time.DateOnly), "elapsed": time.Since(start).Round(time.Millisecond)}).Info("candle agg: done")
	}()

	// Step 1 — Query the 24 CONFIRMED TICK rows for this instrument and day.
	dayEnd := dayStart.Add(24 * time.Hour)

	var tickStates []*ent.State
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.All", "instrument": instrName, "date": dayStart.Format(time.DateOnly)}).Trace("before query")
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
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.All", "err": true}).Trace("after query")
		logrus.WithError(err).Errorf("candle agg %s: query tick states", row.ID)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "query tick states failed")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.All", "count": len(tickStates)}).Trace("after query")

	// Step 2 — Stream each hourly Parquet from R2 into the candle accumulator.
	acc := newCandleAccumulator(dayStart, instrName)
	broken := false

	for _, tickState := range tickStates {
		if ctx.Err() != nil {
			ilogger.T(ctx).WithField("fn", "executeCandleAggregation").Trace("branch: ctx_canceled_in_tick_loop")
			return
		}

		ilogger.T(ctx).WithFields(logrus.Fields{"tick_ts": tickState.Timestamp.Format(time.RFC3339), "is_holiday": tickState.IsHoliday}).Trace("loop: tick state iteration")

		// §4.F-c: skip Zero-Row files (market holidays).
		if tickState.IsHoliday {
			ilogger.T(ctx).WithField("fn", "executeCandleAggregation").Trace("branch: skip_holiday_tick")
			continue
		}

		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "tick_ts": tickState.Timestamp.Format(time.RFC3339)}).Trace("before call: readTickParquetFromR2")
		data, err := readTickParquetFromR2(ctx, instrName, tickState.Timestamp)
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "err": err != nil}).Trace("after call: readTickParquetFromR2")
		if err != nil {
			if isNoSuchKeyError(err) {
				// §4.F-d: NoSuchKey handling.
				if tickState.IsDeleted {
					// Pruned row — safe to skip.
					ilogger.T(ctx).WithField("fn", "executeCandleAggregation").Trace("branch: no_such_key_deleted_skip")
					continue
				}
				// BROKEN Storage: file should exist but doesn't.
				ilogger.T(ctx).WithField("fn", "executeCandleAggregation").Trace("branch: no_such_key_not_deleted_broken")
				logrus.WithFields(logrus.Fields{"state_id": row.ID, "tick_state_id": tickState.ID}).Info("candle agg: NoSuchKey for tick (not deleted)")
				broken = true
				break
			}
			logrus.WithError(err).Errorf("candle agg %s: read tick parquet %s", row.ID, tickState.ID)
			updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "read tick parquet failed")
			return
		}

		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "bytes": len(data)}).Trace("before call: acc.AccumulateTickParquet")
		if err := acc.AccumulateTickParquet(data); err != nil {
			ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "err": true}).Trace("after call: acc.AccumulateTickParquet")
			logrus.WithError(err).Errorf("candle agg %s: accumulate tick %s", row.ID, tickState.ID)
			updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "accumulate tick parquet failed")
			return
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "err": false}).Trace("after call: acc.AccumulateTickParquet")
	}

	if broken {
		ilogger.T(ctx).WithField("fn", "executeCandleAggregation").Trace("branch: broken_storage")
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusBROKEN, "broken storage: tick parquet missing")
		return
	}

	// Step 3 — Build the daily Candles Parquet (all 19 timeframes).
	ilogger.T(ctx).WithField("fn", "executeCandleAggregation").Trace("before call: buildCandleParquet")
	candleData, err := buildCandleParquet(ctx, row, acc)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "err": err != nil}).Trace("after call: buildCandleParquet")
	if err != nil {
		logrus.WithError(err).Errorf("candle agg %s: build candle parquet", row.ID)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "build candle parquet failed")
		return
	}

	// Step 4a — Upload to R2.
	r2Key := ""
	if row.Edges.Instrument != nil {
		r2Key = r2.CandleObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "key": r2Key}).Trace("before r2 upload")
	if err := uploadCandleParquetToR2(ctx, row, candleData); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "key": r2Key, "err": true}).Trace("after r2 upload")
		logrus.WithError(err).Errorf("candle agg %s: upload candle parquet", row.ID)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "upload candle parquet failed")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "key": r2Key, "err": false}).Trace("after r2 upload")
	updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusCOMPLETED, "set candle COMPLETED")

	// Step 4b — Read back and validate.
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "key": r2Key}).Trace("before r2 read-back")
	stored, err := readCandleParquetFromR2(ctx, row)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "err": err != nil}).Trace("after r2 read-back")
	if err != nil {
		logrus.WithError(err).Errorf("candle agg %s: read back candle parquet", row.ID)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusBROKEN, "read-back candle parquet failed")
		return
	}

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "bytes": len(stored)}).Trace("before call: validateCandleParquet")
	if err := validateCandleParquet(ctx, row, stored); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "err": true}).Trace("after call: validateCandleParquet")
		logrus.WithError(err).Errorf("candle agg %s: validate candle parquet", row.ID)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusBROKEN, "validate candle parquet failed")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeCandleAggregation", "err": false}).Trace("after call: validateCandleParquet")

	// Step 4c — Promote to CONFIRMED after readback validation.
	updateSimpleStatus(ctx, d, row, state.PreviousStatusCOMPLETED, state.StatusCONFIRMED, "set candle CONFIRMED")
}

// readTickParquetFromR2 fetches the hourly tick Parquet for the specified instrument and timestamp.
// Returns errNoSuchKey if the R2 object is absent.
func readTickParquetFromR2(ctx context.Context, instrument string, ts time.Time) ([]byte, error) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "readTickParquetFromR2", "instrument": instrument, "ts": ts.Format(time.RFC3339)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "readTickParquetFromR2").Trace("fn_exit")

	if r2Client == nil {
		return nil, errClientsNotInitialized
	}
	key := r2.TickObjectKey(instrument, ts)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "readTickParquetFromR2", "key": key}).Trace("before r2 read")
	data, err := r2Client.GetObject(ctx, key)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "readTickParquetFromR2", "key": key, "err": err != nil}).Trace("after r2 read")
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
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "uploadCandleParquetToR2", "instrument": instrName(row)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "uploadCandleParquetToR2").Trace("fn_exit")

	if r2Client == nil {
		return errClientsNotInitialized
	}
	if row.Edges.Instrument == nil {
		return fmt.Errorf("uploadCandleParquetToR2: instrument not loaded for %s", row.ID)
	}
	key := r2.CandleObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "uploadCandleParquetToR2", "key": key, "bytes": len(data)}).Trace("before r2 upload")
	err := r2Client.PutObject(ctx, key, data)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "uploadCandleParquetToR2", "key": key, "err": err != nil}).Trace("after r2 upload")
	return err
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
