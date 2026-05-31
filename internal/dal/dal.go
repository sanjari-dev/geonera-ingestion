package dal

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sanjari-dev/geonera-ingestion/ent"
)

// Advisory lock IDs — one exclusive slot per process family.
const (
	LockIDTick        int64 = 1001
	LockIDCandle      int64 = 1002
	LockIDMaintenance int64 = 1003
	LockIDSync        int64 = 1004
)

// DAL enforces the split-connection strategy.
//
// lockDB is a direct TCP connection to PostgreSQL that bypasses PgBouncer.
// It is used exclusively by Parent Handlers to hold a transaction-level
// advisory lock for the full duration of an ETL run.
//
// pool is the PgBouncer-routed ent.Client used by all goroutine workers
// for short-lived transactional queries (claim, update, count).
type DAL struct {
	lockDB *sql.DB
	pool   *ent.Client
}

// New constructs a DAL. lockDB must be a direct Postgres connection;
// pool must be routed through PgBouncer.
func New(lockDB *sql.DB, pool *ent.Client) *DAL {
	return &DAL{lockDB: lockDB, pool: pool}
}

// AcquireAdvisoryLock begins a raw SQL transaction on the direct connection.
// The caller must defer tx.Rollback() immediately after this call to guarantee
// the advisory lock is released on return, panic, or context cancellation.
func (d *DAL) AcquireAdvisoryLock(ctx context.Context) (*sql.Tx, error) {
	tx, err := d.lockDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("dal: begin advisory lock tx: %w", err)
	}
	return tx, nil
}

// ExecuteInPool runs fn inside a single transaction on the PgBouncer pool.
// The transaction is automatically rolled back if fn returns an error.
func (d *DAL) ExecuteInPool(ctx context.Context, fn func(tx *ent.Tx) error) error {
	tx, err := d.pool.Tx(ctx)
	if err != nil {
		return fmt.Errorf("dal: begin pool tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
