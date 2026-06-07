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

// ManyForUpdateSkipLocked returns up to `limit` State rows while taking row
// locks with SKIP LOCKED (oldest-claimable-first ordering is the caller's
// responsibility via Order()). It backs the Backfill "Master Bulk-Claim"
// strategy, which locks a whole batch in a single round trip and routes the
// claimed rows to the appropriate handler in memory rather than running
// several independent single-row claim loops.
func (q *StateQuery) ManyForUpdateSkipLocked(ctx context.Context, limit int) ([]*State, error) {
	if err := q.Limit(limit).prepareQuery(ctx); err != nil {
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
	return nodes, nil
}
