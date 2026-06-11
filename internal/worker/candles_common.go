package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"go.opentelemetry.io/otel"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/instrument"
	"github.com/sanjari-dev/geonera-ingestion/ent/predicate"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/candleparquet"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
)

var candleTracer = otel.Tracer("worker/candles")

// RunCandleParentHandler is the single entry point for all candle-aggregation work.
// It holds the global advisory lock for the full duration, so no other job
// (ticks, maintenance, sync) can run concurrently. If the lock is already
//
//	held, the trigger is dropped immediately.
//
// mode must be either "REGULAR" or "BACKFILL".
func RunCandleParentHandler(ctx context.Context, mode string, d *dal.DAL, onStarted func()) bool {
	ctx, span := candleTracer.Start(ctx, fmt.Sprintf("RunCandleParentHandler_%s", mode))
	defer span.End()

	return runWithLocks(ctx, d, "candles/"+mode, []int64{dal.LockIDCandle}, onStarted, func(lockCtx context.Context) {
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
	})
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
	if r2Client == nil {
		return nil, errClientsNotInitialized
	}
	if row.Edges.Instrument == nil {
		return nil, fmt.Errorf("readCandleParquetFromR2: instrument not loaded for %s", row.ID)
	}
	key := r2.CandleObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	data, err := r2Client.GetObject(ctx, key)
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
