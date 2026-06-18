// noinspection SpellCheckingInspection
// Package candleparquet provides Parquet read/write/validate operations for the
// Candle schema defined in architecture §3.B.
package candleparquet

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

const decimalScale = 100_000

// Row is the Go representation of one candle row (one timeframe period).
//
// Architecture §3.B schema (14 columns):
//   - timestamp: int64, microseconds since Unix epoch, UTC (period start)
//   - instrument: string
//   - Timeframe: string, e.g., "m1", "h1", "d1"
//   - open, high, low, close, volume_weighted_average_price: float64
//   - min_spread, max_spread, avg_spread: float64
//   - tick_count: int64
//   - total_bid_volume: int64
//   - total_ask_volume: int64
type Row struct {
	Timestamp                  int64   `parquet:"timestamp"`
	Instrument                 string  `parquet:"instrument"`
	Timeframe                  string  `parquet:"timeframe"`
	Open                       float64 `parquet:"open"`
	High                       float64 `parquet:"high"`
	Low                        float64 `parquet:"low"`
	Close                      float64 `parquet:"close"`
	VolumeWeightedAveragePrice float64 `parquet:"vwap"`
	MinSpread                  float64 `parquet:"min_spread"`
	MaxSpread                  float64 `parquet:"max_spread"`
	AvgSpread                  float64 `parquet:"avg_spread"`
	TickCount                  int64   `parquet:"tick_count"`
	TotalBidVolume             int64   `parquet:"total_bid_volume"`
	TotalAskVolume             int64   `parquet:"total_ask_volume"`
}

type parquetRow struct {
	Timestamp                  int64  `parquet:"timestamp,timestamp(microsecond)"`
	Instrument                 string `parquet:"instrument"`
	Timeframe                  string `parquet:"timeframe"`
	Open                       int64  `parquet:"open,decimal(5:18)"`
	High                       int64  `parquet:"high,decimal(5:18)"`
	Low                        int64  `parquet:"low,decimal(5:18)"`
	Close                      int64  `parquet:"close,decimal(5:18)"`
	VolumeWeightedAveragePrice int64  `parquet:"vwap,decimal(5:18)"`
	MinSpread                  int64  `parquet:"min_spread,decimal(5:18)"`
	MaxSpread                  int64  `parquet:"max_spread,decimal(5:18)"`
	AvgSpread                  int64  `parquet:"avg_spread,decimal(5:18)"`
	TickCount                  int64  `parquet:"tick_count"`
	TotalBidVolume             int64  `parquet:"total_bid_volume"`
	TotalAskVolume             int64  `parquet:"total_ask_volume"`
}

// Timeframe defines one aggregation interval.
type Timeframe struct {
	Name    string
	Minutes int
}

// All19 is the complete, fixed list of 19 canonical timeframes (architecture §2).
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
	"open", "high", "low", "close", "v" + "wap",
	"min_spread", "max_spread", "avg_spread",
	"tick_count", "total_bid_volume", "total_ask_volume",
}

var expectedLogicalTypes = []string{
	"TIMESTAMP(isAdjustedToUTC=true,unit=MICROS)", "", "",
	"DECIMAL(18,5)", "DECIMAL(18,5)", "DECIMAL(18,5)", "DECIMAL(18,5)", "DECIMAL(18,5)",
	"DECIMAL(18,5)", "DECIMAL(18,5)", "DECIMAL(18,5)",
	"", "", "",
}

// Write serializes rows into an in-memory Parquet file and returns the bytes.
// Passing nil or an empty slice produces a valid zero-row file.
func Write(rows []Row) ([]byte, error) {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[parquetRow](&buf)

	if len(rows) > 0 {
		encoded := make([]parquetRow, len(rows))
		for i, row := range rows {
			encoded[i] = toParquetRow(row)
		}
		if _, err := w.Write(encoded); err != nil {
			_ = w.Close()
			return nil, fmt.Errorf("candle parquet write rows: %w", err)
		}
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("candle parquet close writer: %w", err)
	}
	return buf.Bytes(), nil
}

// Validate performs physical checks on Candle Parquet bytes:
//  1. The file size is greater than zero, and PAR1 magic appears in the header and footer.
//  2. The Parquet footer can be parsed.
//  3. The schema has exactly 14 columns in expectedCols order with expected logical types.
//  4. Every row timestamp is inside [dayStart, dayStart+24h).
//  5. Non-empty files contain all 19 canonical timeframes.
func Validate(data []byte, dayStart time.Time) error {
	// 1. Minimum size and PAR1 magic bytes.
	if len(data) < 8 {
		return fmt.Errorf("candle parquet validate: file too small (%d bytes)", len(data))
	}
	if string(data[:4]) != "PAR1" {
		return fmt.Errorf("candle parquet validate: bad header magic %q", data[:4])
	}
	if string(data[len(data)-4:]) != "PAR1" {
		return fmt.Errorf("candle parquet validate: bad footer magic %q", data[len(data)-4:])
	}

	// 2. Parse the footer (validates Parquet structural integrity).
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("candle parquet validate: open file: %w", err)
	}

	// 3. Schema matching.
	if err := checkSchema(f.Schema()); err != nil {
		return err
	}

	// 4–5. Boundary and timeframe coverage checks.
	return checkRows(f, dayStart.UTC())
}

// checkSchema verifies that the file's top-level columns match expectedCols.
func checkSchema(s *parquet.Schema) error {
	fields := s.Fields()
	if len(fields) != len(expectedCols) {
		return fmt.Errorf("candle parquet validate: schema has %d column(s), expected %d", len(fields), len(expectedCols))
	}
	for i, f := range fields {
		if f.Name() != expectedCols[i] {
			return fmt.Errorf("candle parquet validate: column[%d] name %q != expected %q", i, f.Name(), expectedCols[i])
		}
		if expectedLogicalTypes[i] != "" && !strings.Contains(f.String(), expectedLogicalTypes[i]) {
			return fmt.Errorf("candle parquet validate: column[%d] %q logical type %q missing from %q", i, f.Name(), expectedLogicalTypes[i], f.String())
		}
	}
	return nil
}

func checkRows(f *parquet.File, dayStart time.Time) error {
	if f.NumRows() == 0 {
		return nil
	}

	r := parquet.NewGenericReader[parquetRow](f)
	defer func(r *parquet.GenericReader[parquetRow]) {
		_ = r.Close()
	}(r)

	rows := make([]parquetRow, f.NumRows())
	n, err := r.Read(rows)
	if err != nil && err != io.EOF {
		return fmt.Errorf("candle parquet validate: read rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("candle parquet validate: file reports rows but none could be read")
	}

	dayEnd := dayStart.Add(24 * time.Hour)
	seen := make(map[string]struct{}, len(All19))
	for _, row := range rows[:n] {
		ts := time.UnixMicro(row.Timestamp).UTC()
		if ts.Before(dayStart) || !ts.Before(dayEnd) {
			return fmt.Errorf("candle parquet validate: timestamp %s outside window [%s, %s)", ts.Format(time.RFC3339), dayStart.Format(time.RFC3339), dayEnd.Format(time.RFC3339))
		}
		seen[row.Timeframe] = struct{}{}
	}

	for _, tf := range All19 {
		if _, ok := seen[tf.Name]; !ok {
			return fmt.Errorf("candle parquet validate: missing timeframe %q", tf.Name)
		}
	}
	return nil
}

func toParquetRow(row Row) parquetRow {
	return parquetRow{
		Timestamp:                  row.Timestamp,
		Instrument:                 row.Instrument,
		Timeframe:                  row.Timeframe,
		Open:                       encodeDecimal(row.Open),
		High:                       encodeDecimal(row.High),
		Low:                        encodeDecimal(row.Low),
		Close:                      encodeDecimal(row.Close),
		VolumeWeightedAveragePrice: encodeDecimal(row.VolumeWeightedAveragePrice),
		MinSpread:                  encodeDecimal(row.MinSpread),
		MaxSpread:                  encodeDecimal(row.MaxSpread),
		AvgSpread:                  encodeDecimal(row.AvgSpread),
		TickCount:                  row.TickCount,
		TotalBidVolume:             row.TotalBidVolume,
		TotalAskVolume:             row.TotalAskVolume,
	}
}

func encodeDecimal(v float64) int64 {
	return int64(math.Round(v * decimalScale))
}
