package schema

import (
	"context"
	"entgo.io/ent"
	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/entsql"
	entschema "entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"github.com/google/uuid"
	"github.com/sanjari-dev/geonera-ingestion/internal/logger"
)

// State holds the schema definition for the State entity.
// Maps to the ingestion.states table defined in database/init.sql.
// Each row represents one time slot whose data must be fetched from Dukascopy
// for a given instrument and job type (TICK or CANDLE).
type State struct {
	ent.Schema
}

// Fields of the State.
func (State) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).
			Default(uuid.New).
			Immutable(),
		// FK to master.instruments — managed via the "instrument" edge below.
		field.UUID("instrument_id", uuid.UUID{}),
		// Category of ingestion job: TICK or CANDLE.
		field.Enum("job_type").
			Values("TICK", "CANDLE").
			SchemaType(map[string]string{
				dialect.Postgres: "ingestion.job_type_enum",
			}),
		// Start of the time slot (UTC). For TICK jobs this is the top of the hour;
		// for CANDLE jobs it is 00:00:00 UTC.
		field.Time("timestamp").
			SchemaType(map[string]string{
				dialect.Postgres: "timestamptz",
			}),
		// Current lifecycle status of this ingestion slot.
		field.Enum("status").
			Values("PENDING", "PROCESSED", "NOT_FOUND", "FAILED", "COMPLETED", "BROKEN", "CONFIRMED", "ABANDONED").
			SchemaType(map[string]string{
				dialect.Postgres: "ingestion.status_enum",
			}),
		// Status immediately before the most recent transition.
		// Used to detect demotions (e.g., CONFIRMED -> BROKEN) inside the Outbox Worker.
		field.Enum("previous_status").
			Values("PENDING", "PROCESSED", "NOT_FOUND", "FAILED", "COMPLETED", "BROKEN", "CONFIRMED", "ABANDONED").
			Optional().
			Nillable().
			SchemaType(map[string]string{
				dialect.Postgres: "ingestion.status_enum",
			}),
		// Explicit holiday marker. Set to TRUE only when a Zero-Row Parquet is committed.
		field.Bool("is_holiday").
			Default(false),
		// Count of CONFIRMED TICK states for the same instrument and day.
		// CANDLE aggregation triggers when this value reaches 24.
		field.Int("resolved_tick_count").
			Default(0),
		// Cumulative retry count. Transitions to ABANDONED at 5 or above.
		field.Int("retry_count").
			Default(0),
		// Consecutive NOT_FOUND count. Resets to 0 when data is found.
		// Triggers Zero-Row Parquet commitment at 3 or above.
		field.Int("not_found_streak").
			Default(0),
		// Soft-delete mark for the Mark phase of 2-Phase Commit pruning.
		field.Bool("is_deleted").
			Default(false),
		// Timestamp of the last status transition. Set by the application, not a trigger.
		field.Time("updated_at").
			SchemaType(map[string]string{
				dialect.Postgres: "timestamptz",
			}),
		field.String("trace_id").
			Optional().
			Nillable(),
	}
}

// Hooks of the State.
func (State) Hooks() []ent.Hook {
	return []ent.Hook{
		func(next ent.Mutator) ent.Mutator {
			return ent.MutateFunc(func(ctx context.Context, m ent.Mutation) (ent.Value, error) {
				if traceID := logger.TraceIDFromContext(ctx); traceID != "" {
					_ = m.SetField("trace_id", traceID)
				}
				return next.Mutate(ctx, m)
			})
		},
	}
}

// Edges of the State.
func (State) Edges() []ent.Edge {
	return []ent.Edge{
		// M2O: many States belong to one Instrument.
		// Unique() is required so Ent detects this as M2O (not M2M).
		// Field("instrument_id") exposes the FK as a direct struct field.
		edge.From("instrument", Instrument.Type).
			Ref("states").
			Field("instrument_id").
			Unique().
			Required(),
	}
}

// Indexes of the State.
func (State) Indexes() []ent.Index {
	return []ent.Index{
		// Composite unique index: the idempotency key for the entire pipeline.
		// Matches uidx_states_instrument_timestamp_jobtype in init.sql.
		index.Fields("instrument_id", "timestamp", "job_type").
			Unique(),
	}
}

// Annotations of the State.
func (State) Annotations() []entschema.Annotation {
	return []entschema.Annotation{
		entsql.Annotation{
			Table:  "states",
			Schema: "ingestion",
		},
	}
}
