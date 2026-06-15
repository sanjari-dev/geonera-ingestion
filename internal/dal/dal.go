package dal

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sanjari-dev/geonera-ingestion/ent"
)

// DAL wraps the ent.Client and exposes transactional helpers for ETL workers.
type DAL struct {
	db    *ent.Client
	sqlDB *sql.DB
}

// New constructs a DAL from an ent.Client and its underlying *sql.DB.
// Both are required: ent.Client for transactional ORM operations, *sql.DB for
// raw connection access (e.g., advisory locks that must span multiple transactions).
func New(db *ent.Client, sqlDB *sql.DB) *DAL {
	return &DAL{db: db, sqlDB: sqlDB}
}

// Execute runs fn inside a single PostgreSQL transaction.
// The transaction is automatically rolled back if fn returns an error.
func (d *DAL) Execute(ctx context.Context, fn func(tx *ent.Tx) error) error {
	tx, err := d.db.Tx(ctx)
	if err != nil {
		return fmt.Errorf("dal: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// WithAdvisoryLock acquires a PostgreSQL session-level advisory lock identified
// by a key for the duration of fn. Returns (false, nil) without calling fn when
// the lock is already held by another session — the caller should treat this as
// "another worker is handling this resource, skip". Returns (true, fnErr) when
// the lock was acquired; the lock is released before this function returns,
// even if fn panics.
func (d *DAL) WithAdvisoryLock(ctx context.Context, key int64, fn func() error) (bool, error) {
	// A dedicated connection is required so the session-level lock survives
	// across the multiple transactions that fn may open internally.
	conn, err := d.sqlDB.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("advisory lock: get connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	var locked bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&locked); err != nil {
		return false, fmt.Errorf("advisory lock: acquire: %w", err)
	}
	if !locked {
		return false, nil
	}
	// Use Background context for unlocking — ctx may already be canceled by the
	// time fn returns, but the lock must always be released.
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, key)
	}()

	return true, fn()
}
