package dal

import (
	"context"
	"fmt"

	"github.com/sanjari-dev/geonera-ingestion/ent"
)

// Advisory lock IDs for the smart-lock compatibility matrix.
//
//	Ticks       (1001) — held by ticks/regular and ticks/backfill
//	Candle      (1002) — held by candles/regular and candles/backfill
//	Sync        (1004) — held by sync only (prevents sync+sync overlap)
//	Maintenance acquires BOTH LockIDTick + LockIDCandle, which means it can
//	   only run when no ticks AND no candles job is running, and vice-versa.
//
// Ticks and Candles can run simultaneously (different lock IDs).
// Sync is independent of ticks/candles and may overlap with them.
const (
	LockIDTick   int64 = 1001
	LockIDCandle int64 = 1002
	LockIDSync   int64 = 1004
)

// DAL enforces the split-connection strategy.
//
// lock is a direct TCP connection to PostgreSQL that bypasses PgBouncer.
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
