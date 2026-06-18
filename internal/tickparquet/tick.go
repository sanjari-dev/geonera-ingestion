// Package tickparquet Package tick parquet provides Parquet read/write/validate operations for the
// hourly Tick schema defined in architecture §3.A.
package tickparquet

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

// Row is the canonical Go representation of one row in a Tick Parquet file.
//
// Architecture §3.A schema:
//   - Timestamp: int64 — microseconds since Unix epoch, UTC
//   - Instrument: string — instrument name (e.g., "eurusd")
//   - Bid / ask: float64 — actual price after divider (≈ Decimal precision)
//   - Bid_volume / ask_volume: int64
type Row struct {
	Timestamp  int64   `parquet:"timestamp"`
	Instrument string  `parquet:"instrument"`
	Bid        float64 `parquet:"bid"`
	Ask        float64 `parquet:"ask"`
	BidVolume  int64   `parquet:"bid_volume"`
	AskVolume  int64   `parquet:"ask_volume"`
}

type parquetRow struct {
	Timestamp  int64  `parquet:"timestamp,timestamp(microsecond)"`
	Instrument string `parquet:"instrument"`
	Bid        int64  `parquet:"bid,decimal(5:18)"`
	Ask        int64  `parquet:"ask,decimal(5:18)"`
	BidVolume  int64  `parquet:"bid_volume"`
	AskVolume  int64  `parquet:"ask_volume"`
}

// expectedCols is the ordered list of column names the file schema must have.
var expectedCols = []string{
	"timestamp", "instrument", "bid", "ask", "bid_volume", "ask_volume",
}

var expectedLogicalTypes = []string{
	"TIMESTAMP(isAdjustedToUTC=true,unit=MICROS)", "",
	"DECIMAL(18,5)", "DECIMAL(18,5)",
	"", "",
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
			return nil, fmt.Errorf("tickparquet write rows: %w", err)
		}
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("tickparquet close writer: %w", err)
	}
	return buf.Bytes(), nil
}

// WriteZeroRow returns a valid Parquet file with the Tick schema but 0 data rows.
// Used when marking a confirmed holiday hour (IsHoliday = true).
func WriteZeroRow() ([]byte, error) {
	return Write(nil)
}

// Validate performs the four-step physical validation from architecture §6.3:
//
//  1. File size > 0 and PAR1 magic bytes are present (header and footer).
//  2. Parquet footer can be parsed (footer integrity).
//  3. Column names and count match the expected Tick schema.
//  4. If rowCount > 0: first and last timestamps lie within [hourStart, hourStart+1h).
//
// Special case (§6.3 §4 exception): if rowCount == 0 and isHoliday is true,
// only checks 1–3.  If rowCount == 0 and isHoliday is false, the file is rejected.
func Validate(data []byte, isHoliday bool, hourStart time.Time) error {
	// 1. Minimum size and PAR1 magic number.
	if len(data) < 8 {
		return fmt.Errorf("tickparquet validate: file too small (%d bytes)", len(data))
	}
	if string(data[:4]) != "PAR1" {
		return fmt.Errorf("tickparquet validate: bad header magic %q", data[:4])
	}
	if string(data[len(data)-4:]) != "PAR1" {
		return fmt.Errorf("tickparquet validate: bad footer magic %q", data[len(data)-4:])
	}

	// 2. Parse the footer (validates Parquet structural integrity).
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("tickparquet validate: open file: %w", err)
	}

	// 3. Schema matching.
	if err := checkSchema(f.Schema()); err != nil {
		return err
	}

	numRows := f.NumRows()

	// Zero-row context (§6.3 exception).
	if numRows == 0 {
		if !isHoliday {
			return fmt.Errorf("tickparquet validate: zero rows but IsHoliday=false (unexpected empty file)")
		}
		return nil // holiday zero-row: magic + schema checks are enough
	}

	// 4. Data boundary check — timestamps within [hourStart, hourStart+1h).
	return checkBoundary(f, hourStart)
}

// ReadAll reads and returns all tick rows from the given Parquet bytes.
// Returns nil for zero-row (holiday) files.
func ReadAll(data []byte) ([]Row, error) {
	if len(data) == 0 {
		return nil, nil
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("tickparquet read: open file: %w", err)
	}
	n := f.NumRows()
	if n == 0 {
		return nil, nil
	}
	encoded := make([]parquetRow, n)
	r := parquet.NewGenericReader[parquetRow](f)
	defer func(r *parquet.GenericReader[parquetRow]) {
		_ = r.Close()
	}(r)
	got, err := r.Read(encoded)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("tickparquet read rows: %w", err)
	}
	rows := make([]Row, got)
	for i := range encoded[:got] {
		rows[i] = fromParquetRow(encoded[i])
	}
	return rows[:got], nil
}

// checkSchema verifies that the file's top-level columns match expectedCols.
func checkSchema(s *parquet.Schema) error {
	fields := s.Fields()
	if len(fields) != len(expectedCols) {
		return fmt.Errorf("tickparquet validate: schema has %d column(s), expected %d", len(fields), len(expectedCols))
	}
	for i, f := range fields {
		if f.Name() != expectedCols[i] {
			return fmt.Errorf("tickparquet validate: column[%d] name %q != expected %q", i, f.Name(), expectedCols[i])
		}
		if expectedLogicalTypes[i] != "" && !strings.Contains(f.String(), expectedLogicalTypes[i]) {
			return fmt.Errorf("tickparquet validate: column[%d] %q logical type %q missing from %q", i, f.Name(), expectedLogicalTypes[i], f.String())
		}
	}
	return nil
}

// checkBoundary reads all rows and verifies that the first and last tick
// timestamps fall within [hourStart, hourStart+1h).
func checkBoundary(f *parquet.File, hourStart time.Time) error {
	r := parquet.NewGenericReader[parquetRow](f)
	defer func(r *parquet.GenericReader[parquetRow]) {
		_ = r.Close()
	}(r)

	rows := make([]parquetRow, f.NumRows())
	n, err := r.Read(rows)
	if err != nil && err != io.EOF {
		return fmt.Errorf("tick parquet validate: read rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("tick parquet validate: file reports rows but none could be read")
	}

	hourEnd := hourStart.Add(time.Hour)
	first := time.UnixMicro(rows[0].Timestamp).UTC()
	last := time.UnixMicro(rows[n-1].Timestamp).UTC()

	if first.Before(hourStart) || !first.Before(hourEnd) {
		return fmt.Errorf("tick parquet validate: first tick %s outside window [%s, %s)", first.Format(time.RFC3339), hourStart.Format(time.RFC3339), hourEnd.Format(time.RFC3339))
	}
	if last.Before(hourStart) || !last.Before(hourEnd) {
		return fmt.Errorf("tick parquet validate: last tick %s outside window [%s, %s)", last.Format(time.RFC3339), hourStart.Format(time.RFC3339), hourEnd.Format(time.RFC3339))
	}
	return nil
}

func toParquetRow(row Row) parquetRow {
	return parquetRow{
		Timestamp:  row.Timestamp,
		Instrument: row.Instrument,
		Bid:        encodeDecimal(row.Bid),
		Ask:        encodeDecimal(row.Ask),
		BidVolume:  row.BidVolume,
		AskVolume:  row.AskVolume,
	}
}

func fromParquetRow(row parquetRow) Row {
	return Row{
		Timestamp:  row.Timestamp,
		Instrument: row.Instrument,
		Bid:        decodeDecimal(row.Bid),
		Ask:        decodeDecimal(row.Ask),
		BidVolume:  row.BidVolume,
		AskVolume:  row.AskVolume,
	}
}

func encodeDecimal(v float64) int64 {
	return int64(math.Round(v * decimalScale))
}

func decodeDecimal(v int64) float64 {
	return float64(v) / decimalScale
}
