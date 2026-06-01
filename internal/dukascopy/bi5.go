package dukascopy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/ulikunitz/xz/lzma"
)

const bi5RecordBytes = 20

// Tick holds the values of one parsed BI5 tick record.
type Tick struct {
	Timestamp time.Time
	Ask       float64 // actual price = bi5_integer / divider
	Bid       float64
	AskVolume float64 // decoded from IEEE-754 float32
	BidVolume float64
}

// ParseBI5 decompresses an LZMA-compressed BI5 payload and returns the ticks.
//
// hourUTC is the UTC start-of-hour that the file represents.
// divider is the instrument's price divisor (e.g., 100,000 for EUR/USD with 5
// decimal places, 1,000 for XAU/USD or USD/JPY with 3 decimal places).
//
// BI5 record layout — big-endian, 20 bytes per tick:
//
//	[0:4] uint32 milliseconds from hourUTC start
//	[4:8] uint32 ask price integer (divide by divider for actual price)
//	[8:12] uint32 bid price integer
//	[12:16] float32 ask volume
//	[16:20] float32 bid volume
func ParseBI5(compressed []byte, hourUTC time.Time, divider int) ([]Tick, error) {
	if len(compressed) == 0 {
		return nil, nil
	}

	// Decompress LZMA1 stream (Dukascopy BI5 uses LZMA1, not LZMA2/XZ).
	lr, err := lzma.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("bi5: LZMA reader: %w", err)
	}
	raw, err := io.ReadAll(lr)
	if err != nil {
		return nil, fmt.Errorf("bi5: LZMA decompress: %w", err)
	}
	if len(raw)%bi5RecordBytes != 0 {
		return nil, fmt.Errorf("bi5: decompressed %d bytes is not a multiple of %d", len(raw), bi5RecordBytes)
	}

	count := len(raw) / bi5RecordBytes
	ticks := make([]Tick, 0, count)
	base := hourUTC.UTC().Truncate(time.Hour)

	div := float64(divider)
	if div == 0 {
		div = 100_000 // safe default for 5-decimal FX pairs
	}

	for i := range count {
		rec := raw[i*bi5RecordBytes : (i+1)*bi5RecordBytes]
		ticks = append(ticks, Tick{
			Timestamp: base.Add(time.Duration(binary.BigEndian.Uint32(rec[0:4])) * time.Millisecond),
			Ask:       float64(binary.BigEndian.Uint32(rec[4:8])) / div,
			Bid:       float64(binary.BigEndian.Uint32(rec[8:12])) / div,
			AskVolume: float64(math.Float32frombits(binary.BigEndian.Uint32(rec[12:16]))),
			BidVolume: float64(math.Float32frombits(binary.BigEndian.Uint32(rec[16:20]))),
		})
	}
	return ticks, nil
}
