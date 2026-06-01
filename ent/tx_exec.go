package ent

import (
	"context"
	"database/sql"
)

// ExecContext executes raw SQL on the current ent transaction.
// Use this sparingly for SQL features not exposed by the generated builders.
func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) error {
	return tx.config.driver.Exec(ctx, query, args, nil)
}

// QueryContext executes raw SQL on the current ent transaction and returns rows.
// The caller is responsible for closing the returned rows.
func (tx *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	var rows sql.Rows
	if err := tx.config.driver.Query(ctx, query, args, &rows); err != nil {
		return nil, err
	}
	return &rows, nil
}

// QueryBoolContext executes a query that returns a single boolean column.
func (tx *Tx) QueryBoolContext(ctx context.Context, query string, args ...any) (bool, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return false, sql.ErrNoRows
	}
	var value bool
	if err := rows.Scan(&value); err != nil {
		return false, err
	}
	return value, rows.Err()
}
