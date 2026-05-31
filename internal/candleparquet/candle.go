// Package candleparquet provides Parquet read/write/validate operations for the
// Candle schema defined in architecture §3.B.
package candleparquet

import (
	"bytes"
	"fmt"

	parquet "github.com/parquet-go/parquet-go"
)

// Row is the Go representation of one candle row (one timeframe period).
//
// Architecture §3.B schema (14 columns):
//   - timestamp:        int64  — microseconds since Unix epoch, UTC (period start)
//   - instrument:       string
//   - timeframe:        string — e.g. "m1", "h1", "d1"
//   - open/high/low/close/vwap: float64
//   - min_spread/max_spread/avg_spread: float64
//   - tick_count:       int64
//   - total_bid_volume: int64
//   - total_ask_volume: int64
type Row struct {
	Timestamp      int64   `parquet:"timestamp"`
	Instrument     string  `parquet:"instrument"`
	Timeframe      string  `parquet:"timeframe"`
	Open           float64 `parquet:"open"`
	High           float64 `parquet:"high"`
	Low            float64 `parquet:"low"`
	Close          float64 `parquet:"close"`
	VWAP           float64 `parquet:"vwap"`
	MinSpread      float64 `parquet:"min_spread"`
	MaxSpread      float64 `parquet:"max_spread"`
	AvgSpread      float64 `parquet:"avg_spread"`
	TickCount      int64   `parquet:"tick_count"`
	TotalBidVolume int64   `parquet:"total_bid_volume"`
	TotalAskVolume int64   `parquet:"total_ask_volume"`
}

// Timeframe defines one aggregation interval.
type Timeframe struct {
	Name    string
	Minutes int
}

// All19 is the complete fixed list of 19 canonical timeframes (architecture §2).
var All19 = []Timeframe{
	{Name: "m1", Minutes: 1},
	{Name: "m2", Minutes: 2},
	{Name: "m3", Minutes: 3},
	{Name: "m4", Minutes: 4},
	{Name: "m5", Minutes: 5},
	{Name: "m6", Minutes: 6},
	{Name: "m10", Minutes: 10},
	{Name: "m12", Minutes: 12},
	{Name: "m15", Minutes: 15},
	{Name: "m20", Minutes: 20},
	{Name: "m30", Minutes: 30},
	{Name: "h1", Minutes: 60},
	{Name: "h2", Minutes: 120},
	{Name: "h3", Minutes: 180},
	{Name: "h4", Minutes: 240},
	{Name: "h6", Minutes: 360},
	{Name: "h8", Minutes: 480},
	{Name: "h12", Minutes: 720},
	{Name: "d1", Minutes: 1440},
}

// expectedCols lists the 14 column names in schema order.
var expectedCols = []string{
	"timestamp", "instrument", "timeframe",
	"open", "high", "low", "close", "vwap",
	"min_spread", "max_spread", "avg_spread",
	"tick_count", "total_bid_volume", "total_ask_volume",
}

// Write serialises rows into an in-memory Parquet file and returns the bytes.
// Passing nil or an empty slice produces a valid zero-row file.
func Write(rows []Row) ([]byte, error) {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[Row](&buf)

	if len(rows) > 0 {
		if _, err := w.Write(rows); err != nil {
			_ = w.Close()
			return nil, fmt.Errorf("candleparquet write rows: %w", err)
		}
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("candleparquet close writer: %w", err)
	}
	return buf.Bytes(), nil
}

// Validate performs physical checks on Candle Parquet bytes:
//  1. File size > 0 and PAR1 magic header/footer.
//  2. Parquet footer can be parsed (footer integrity).
//  3. Schema: exactly 14 columns in expectedCols order.
func Validate(data []byte) error {
	// 1. Minimum size and PAR1 magic bytes.
	if len(data) < 8 {
		return fmt.Errorf("candleparquet validate: file too small (%d bytes)", len(data))
	}
	if string(data[:4]) != "PAR1" {
		return fmt.Errorf("candleparquet validate: bad header magic %q", data[:4])
	}
	if string(data[len(data)-4:]) != "PAR1" {
		return fmt.Errorf("candleparquet validate: bad footer magic %q", data[len(data)-4:])
	}

	// 2. Parse the footer (validates Parquet structural integrity).
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("candleparquet validate: open file: %w", err)
	}

	// 3. Schema matching.
	return checkSchema(f.Schema())
}

// checkSchema verifies that the file's top-level columns match expectedCols.
func checkSchema(s *parquet.Schema) error {
	fields := s.Fields()
	if len(fields) != len(expectedCols) {
		return fmt.Errorf("candleparquet validate: schema has %d column(s), expected %d",
			len(fields), len(expectedCols))
	}
	for i, f := range fields {
		if f.Name() != expectedCols[i] {
			return fmt.Errorf("candleparquet validate: column[%d] name %q != expected %q",
				i, f.Name(), expectedCols[i])
		}
	}
	return nil
}
