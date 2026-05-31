package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	entschema "entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"github.com/google/uuid"
)

// Timeframe holds the schema definition for the Timeframe entity.
// Maps to the master.timeframes table defined in database/init.sql.
// This is a fixed catalogue of 19 standard resolutions; it must not be modified at runtime.
type Timeframe struct {
	ent.Schema
}

// Fields of the Timeframe.
func (Timeframe) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("id", uuid.UUID{}).
			Default(uuid.New).
			Immutable(),
		// Timeframe label, e.g., m1, h1, d1.
		field.String("name").
			Unique().
			NotEmpty(),
		// Duration of the timeframe in minutes (e.g., m1=1, h1=60, d1=1440).
		field.Int("minutes").
			Positive(),
		field.Bool("is_active").
			Default(true),
	}
}

// Edges of the Timeframe.
func (Timeframe) Edges() []ent.Edge {
	return nil
}

// Annotations of the Timeframe.
func (Timeframe) Annotations() []entschema.Annotation {
	return []entschema.Annotation{
		entsql.Annotation{
			Table:  "timeframes",
			Schema: "master",
		},
	}
}
