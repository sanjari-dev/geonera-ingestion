package worker

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/sirupsen/logrus"
	"sync"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/instrument"
	"github.com/sanjari-dev/geonera-ingestion/ent/predicate"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/candleparquet"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	ilogger "github.com/sanjari-dev/geonera-ingestion/internal/logger"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
)

// candleAggSem limits concurrent candle aggregation goroutines to runtime.NumCPU().
// Each goroutine streams up to 24 hourly Parquet files from R2; bounding concurrency
// prevents R2 connection spikes and caps peak memory under the aggregation loop.
var candleAggSem = make(chan struct{}, runtime.NumCPU())

// RunCandleParentHandler is the single entry point for all candle-aggregation work.
// mode must be either "REGULAR" or "BACKFILL".
func RunCandleParentHandler(ctx context.Context, mode string, d *dal.DAL, onStarted func()) bool {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "RunCandleParentHandler", "mode": mode}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "RunCandleParentHandler").Trace("fn_exit")

	if onStarted != nil {
		onStarted()
	}
	var wg sync.WaitGroup
	switch mode {
	case "REGULAR":
		ilogger.T(ctx).WithField("mode", mode).Trace("branch: candle_regular")
		wg.Add(1)
		go runCandleRegular(ctx, d, &wg)
	case "BACKFILL":
		ilogger.T(ctx).WithField("mode", mode).Trace("branch: candle_backfill")
		wg.Add(1)
		go runCandleBackfill(ctx, d, &wg)
	default:
		logrus.WithField("mode", mode).Warn("candles: unknown mode")
	}
	ilogger.T(ctx).WithField("fn", "RunCandleParentHandler").Trace("before wg.Wait")
	wg.Wait()
	ilogger.T(ctx).WithField("fn", "RunCandleParentHandler").Trace("after wg.Wait")
	return true
}

func candleStateRows(q *ent.StateQuery, filters ...predicate.State) *ent.StateQuery {
	baseFilters := []predicate.State{
		state.JobTypeEQ(state.JobTypeCANDLE),
		state.HasInstrumentWith(
			instrument.IsActiveEQ(true),
			instrument.IsPauseEQ(false),
		),
		state.IsDeletedEQ(false),
	}
	return q.Where(append(baseFilters, filters...)...)
}

// errNoSuchKey mirrors r2.ErrNotFound so that candle aggregation can detect
// a missing tick Parquet and apply the NoSuchKey handling rules from §4.F.
var errNoSuchKey = r2.ErrNotFound

func isNoSuchKeyError(err error) bool { return errors.Is(err, r2.ErrNotFound) }

// readCandleParquetFromR2 fetches the daily Candle Parquet from R2 for validation.
func readCandleParquetFromR2(ctx context.Context, row *ent.State) ([]byte, error) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "readCandleParquetFromR2", "instrument": instrName(row)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "readCandleParquetFromR2").Trace("fn_exit")

	if r2Client == nil {
		return nil, errClientsNotInitialized
	}
	if row.Edges.Instrument == nil {
		return nil, fmt.Errorf("readCandleParquetFromR2: instrument not loaded for %s", row.ID)
	}
	key := r2.CandleObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "readCandleParquetFromR2", "key": key}).Trace("before r2 read")
	data, err := r2Client.GetObject(ctx, key)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "readCandleParquetFromR2", "key": key, "err": err != nil}).Trace("after r2 read")
	if errors.Is(err, r2.ErrNotFound) {
		return nil, fmt.Errorf("readCandleParquetFromR2: key not found: %s", key)
	}
	return data, err
}

// validateCandleParquet runs physical validation on the Candle Parquet bytes:
// magic number, schema, timeframe coverage, and boundary checks.
func validateCandleParquet(_ context.Context, row *ent.State, data []byte) error {
	return candleparquet.Validate(data, row.Timestamp.UTC())
}
