package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/entsql"
	entschema "entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"github.com/google/uuid"
)

// SyncTask holds the schema definition for the SyncTask entity.
// Maps to the ingestion.sync_tasks table defined in database/init.sql.
// Implements the Transactional Outbox Pattern: when a TICK State reaches a
// terminal status, an event row is upserted here atomically. The Outbox Worker
// consumes PENDING rows to recompute resolved_tick_count on CANDLE states.
type SyncTask struct {
	ent.Schema
}

// Fields of the SyncTask.
func (SyncTask) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).
			Default(uuid.New).
			Immutable(),
		// FK to master.instruments — managed via the "instrument" edge below.
		field.UUID("instrument_id", uuid.UUID{}),
		// Calendar day (00:00:00 UTC) of the TICK slot that triggered this sync event.
		field.Time("target_date").
			SchemaType(map[string]string{
				dialect.Postgres: "timestamptz",
			}),
		// Lifecycle status of this outbox event.
		field.Enum("status").
			Values("PENDING", "PROCESSED").
			Default("PENDING").
			SchemaType(map[string]string{
				dialect.Postgres: "ingestion.sync_status_enum",
			}),
		field.Time("created_at").
			Default(time.Now).
			Immutable().
			SchemaType(map[string]string{
				dialect.Postgres: "timestamptz",
			}),
	}
}

// Edges of the SyncTask.
func (SyncTask) Edges() []ent.Edge {
	return []ent.Edge{
		// M2O: many SyncTasks belong to one Instrument.
		// Unique() is required so Ent detects this as M2O (not M2M).
		// Field("instrument_id") exposes the FK as a direct struct field.
		edge.From("instrument", Instrument.Type).
			Ref("sync_tasks").
			Field("instrument_id").
			Unique().
			Required(),
	}
}

// Indexes of the SyncTask.
func (SyncTask) Indexes() []ent.Index {
	return []ent.Index{
		// Composite unique index: at most one pending sync event per instrument per day.
		// Matches uidx_sync_tasks_instrument_target_date in init.sql.
		index.Fields("instrument_id", "target_date").
			Unique(),
	}
}

// Annotations of the SyncTask.
func (SyncTask) Annotations() []entschema.Annotation {
	return []entschema.Annotation{
		entsql.Annotation{
			Table:  "sync_tasks",
			Schema: "ingestion",
		},
	}
}
