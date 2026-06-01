package ent

import (
	"context"

	entsql "entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/sqlgraph"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
)

// FirstForUpdateSkipLocked returns the first State row while taking a row lock
// with SKIP LOCKED. It is intended for short claim transactions.
func (q *StateQuery) FirstForUpdateSkipLocked(ctx context.Context) (*State, error) {
	if err := q.Limit(1).prepareQuery(ctx); err != nil {
		return nil, err
	}
	nodes, err := q.sqlAll(ctx, func(_ context.Context, spec *sqlgraph.QuerySpec) {
		spec.Modifiers = append(spec.Modifiers, func(s *entsql.Selector) {
			s.ForUpdate(entsql.WithLockAction(entsql.SkipLocked))
		})
	})
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, &NotFoundError{state.Label}
	}
	return nodes[0], nil
}
