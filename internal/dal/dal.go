package dal

import (
	"context"
	"fmt"

	"github.com/sanjari-dev/geonera-ingestion/ent"
)

// Advisory lock IDs for the smart-lock compatibility matrix.
//
//	Ticks (1001) — held by ticks/regular and ticks/backfill
//	Candle (1002) — held by candles/regular and candles/backfill
//	Maintenance (1003) — held by maintenance only
//	Sync (1004) — held by sync only (prevents sync+sync overlap)
//
// Ticks and Candles can run simultaneously (different lock IDs).
// Maintenance uses its own dedicated lock so it can run concurrently with
// Ticks and Candles. All maintenance inserts use ON CONFLICT DO NOTHING;
// IsActive=false guards the bootstrap path against worker interference.
// Sync is independent of all other workers and may overlap freely.
const (
	LockIDTick        int64 = 1001
	LockIDCandle      int64 = 1002
	LockIDMaintenance int64 = 1003
	LockIDSync        int64 = 1004
)

// DAL enforces the split-connection strategy.
//
// Lock is a direct TCP connection to PostgreSQL that bypasses PgBouncer.
// It is used exclusively by Parent Handlers to hold a transaction-level
// advisory lock for the full duration of an ETL run.
//
// Pool is the PgBouncer-routed ent.Client used by all goroutine workers
// for short-lived transactional queries (claim, update, count).
type DAL struct {
	lock *ent.Client
	pool *ent.Client
}

// New constructs a DAL. lockDB must be a direct Postgres connection;
// pool must be routed through PgBouncer.
func New(lock *ent.Client, pool *ent.Client) *DAL {
	return &DAL{lock: lock, pool: pool}
}

// AcquireAdvisoryLock begins an ent transaction on the direct connection.
// The caller must defer tx.Rollback() immediately after this call to guarantee
// the advisory lock is released on return, panic, or context cancellation.
func (d *DAL) AcquireAdvisoryLock(ctx context.Context) (*ent.Tx, error) {
	tx, err := d.lock.Tx(ctx)
	if err != nil {
		return nil, fmt.Errorf("dal: begin advisory lock tx: %w", err)
	}
	return tx, nil
}

// WaitAndAcquireAdvisoryLock begins a transaction on the direct connection and
// blocks until advisory lock id is acquired (pg_advisory_xact_lock — no timeout,
// no try-and-fail). Unlike AcquireAdvisoryLock + runWithLocks, this does NOT use
// pg_try_advisory_xact_lock, so it will wait rather than drop the trigger.
// The caller must defer tx.Rollback() immediately after this call.
func (d *DAL) WaitAndAcquireAdvisoryLock(ctx context.Context, id int64) (*ent.Tx, error) {
	tx, err := d.lock.Tx(ctx)
	if err != nil {
		return nil, fmt.Errorf("dal: begin advisory lock tx: %w", err)
	}
	// pg_advisory_xact_lock blocks until the lock is available.
	// CTE wrapper makes the void return value scannable as bool.
	if _, err := tx.QueryBoolContext(ctx,
		"WITH l AS (SELECT pg_advisory_xact_lock($1::bigint)) SELECT true FROM l", id,
	); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("dal: pg_advisory_xact_lock(%d): %w", id, err)
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
