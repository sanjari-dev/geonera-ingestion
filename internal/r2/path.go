package r2

import (
	"fmt"
	"time"
)

// TickObjectKey returns the canonical R2 object key for a Tick Parquet file.
//
//	ingestion/dukascopy/ticks/{instrument}/{YYYY}/{MM}/ticks-{instrument}-{YYYY-MM-DD}-{HH}.parquet
func TickObjectKey(instrument string, ts time.Time) string {
	t := ts.UTC()
	return fmt.Sprintf(
		"ingestion/dukascopy/ticks/%s/%04d/%02d/ticks-%s-%04d-%02d-%02d-%02d.parquet",
		instrument,
		t.Year(), int(t.Month()),
		instrument,
		t.Year(), int(t.Month()), t.Day(), t.Hour(),
	)
}

// CandleObjectKey returns the canonical R2 object key for a Candle Parquet file.
//
//	ingestion/dukascopy/candles/{instrument}/{YYYY}/candles-{instrument}-{YYYY-MM-DD}.parquet
func CandleObjectKey(instrument string, ts time.Time) string {
	t := ts.UTC()
	return fmt.Sprintf(
		"ingestion/dukascopy/candles/%s/%04d/candles-%s-%04d-%02d-%02d.parquet",
		instrument,
		t.Year(),
		instrument,
		t.Year(), int(t.Month()), t.Day(),
	)
}
