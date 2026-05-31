package seed

import (
	"context"
	"fmt"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/timeframe"
)

// timeframeRows defines the fixed catalogue of 19 standard timeframes.
// This list is immutable — do not add, remove, or rename entries.
var timeframeRows = []struct {
	Name    string
	Minutes int
}{
	{"m1", 1},
	{"m2", 2},
	{"m3", 3},
	{"m4", 4},
	{"m5", 5},
	{"m6", 6},
	{"m10", 10},
	{"m12", 12},
	{"m15", 15},
	{"m20", 20},
	{"m30", 30},
	{"h1", 60},
	{"h2", 120},
	{"h3", 180},
	{"h4", 240},
	{"h6", 360},
	{"h8", 480},
	{"h12", 720},
	{"d1", 1440},
}

// Timeframes insert the 19 standard timeframes into master.timeframes.
// It is idempotent: rows that already exist (matched by name) are skipped.
// Call this once during application startup after the database connection
// has been established.
func Timeframes(ctx context.Context, client *ent.Client) error {
	for _, row := range timeframeRows {
		exists, err := client.Timeframe.
			Query().
			Where(timeframe.NameEQ(row.Name)).
			Exist(ctx)
		if err != nil {
			return fmt.Errorf("seed timeframes: check existence of %q: %w", row.Name, err)
		}
		if exists {
			continue
		}

		if err := client.Timeframe.
			Create().
			SetName(row.Name).
			SetMinutes(row.Minutes).
			SetIsActive(true).
			Exec(ctx); err != nil {
			return fmt.Errorf("seed timeframes: insert %q: %w", row.Name, err)
		}
	}
	return nil
}
