package schema

import (
	"context"
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/entsql"
	entschema "entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"github.com/google/uuid"
)

// Instrument holds the schema definition for the Instrument entity.
// Maps to the master.instruments table defined in database/init.sql.
type Instrument struct {
	ent.Schema
}

// Fields of the Instrument.
func (Instrument) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).
			Default(uuid.New).
			Immutable(),
		field.String("name").
			Unique().
			NotEmpty(),
		field.String("description").
			NotEmpty(),
		field.String("asset_class").
			NotEmpty(),
		// Whether the instrument is currently active in the ingestion pipeline.
		field.Bool("is_active").
			Default(true),
		// Divisor applied to the raw integer price from the Dukascopy BI5 feed
		// (e.g., 100,000 for 5-decimal FX pairs such as EURUSD).
		field.Int("divider").
			Positive(),
		// Earliest date for which historical data is available at Dukascopy (UTC).
		field.Time("start_date").
			SchemaType(map[string]string{
				dialect.Postgres: "timestamptz",
			}),
		// Mutex flag: TRUE pauses the pipeline for this instrument pending manual resolution.
		field.Bool("is_pause").
			Default(false),
	}
}

// Hooks of the Instrument.
func (Instrument) Hooks() []ent.Hook {
	return []ent.Hook{
		func(next ent.Mutator) ent.Mutator {
			return ent.MutateFunc(func(ctx context.Context, m ent.Mutation) (ent.Value, error) {
				if m.Op().Is(ent.OpCreate | ent.OpUpdate | ent.OpUpdateOne) {
					if value, exists := m.Field("start_date"); exists {
						if startDate, ok := value.(time.Time); ok {
							if err := m.SetField("start_date", startDate.UTC()); err != nil {
								return nil, err
							}
						}
					}
				}
				return next.Mutate(ctx, m)
			})
		},
	}
}

// Edges of the Instrument.
func (Instrument) Edges() []ent.Edge {
	return []ent.Edge{
		// O2M: one Instrument owns many States.
		edge.To("states", State.Type),
		// O2M: one Instrument owns many SyncTasks.
		edge.To("sync_tasks", SyncTask.Type),
	}
}

// Annotations of the Instrument.
func (Instrument) Annotations() []entschema.Annotation {
	return []entschema.Annotation{
		entsql.Annotation{
			Table:  "instruments",
			Schema: "master",
		},
	}
}
