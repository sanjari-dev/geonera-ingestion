package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/sanjari-dev/geonera-ingestion/ent"

	_ "github.com/lib/pq"
)

// NewEntClient opens a PostgreSQL connection using DATABASE_DIRECT_URL from the
// environment, verifies connectivity with Ping, then returns an Ent client.
// The caller is responsible for closing the client when done.
func NewEntClient(ctx context.Context) (*ent.Client, error) {
	dsn := os.Getenv("DATABASE_DIRECT_URL")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_DIRECT_URL is not set")
	}

	if err := ping(ctx, dsn); err != nil {
		return nil, err
	}

	client, err := ent.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open ent client: %w", err)
	}

	return client, nil
}

// NewLockDB opens a raw *sql.DB on the direct Postgres connection (DATABASE_DIRECT_URL).
// This bypasses PgBouncer and is reserved exclusively for the Parent Handlers
// that must hold a transaction-level advisory lock for the duration of an ETL run.
// The caller is responsible for closing the DB when done.
func NewLockDB(ctx context.Context) (*sql.DB, error) {
	dsn := os.Getenv("DATABASE_DIRECT_URL")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_DIRECT_URL is not set")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock db: %w", err)
	}

	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping lock db: %w", err)
	}

	return db, nil
}

// NewPoolClient opens an ent.Client for ETL worker goroutines.
//
// Resolution order:
//  1. DATABASE_POOL_URL (PgBouncer) — preferred in production.
//  2. DATABASE_DIRECT_URL — fallback when PgBouncer is absent or unreachable
//     (acceptable in local development; logs a warning).
//
// The caller is responsible for closing the client when done.
func NewPoolClient(ctx context.Context) (*ent.Client, error) {
	poolDSN := os.Getenv("DATABASE_POOL_URL")
	if poolDSN != "" {
		if err := ping(ctx, poolDSN); err == nil {
			return openEnt(poolDSN)
		}
		log.Printf("database: DATABASE_POOL_URL unreachable (%s), falling back to DATABASE_DIRECT_URL (dev mode)", os.Getenv("DATABASE_POOL_URL"))
	} else {
		log.Println("database: DATABASE_POOL_URL not set, falling back to DATABASE_DIRECT_URL (dev mode)")
	}

	directDSN := os.Getenv("DATABASE_DIRECT_URL")
	if directDSN == "" {
		return nil, fmt.Errorf("neither DATABASE_POOL_URL nor DATABASE_DIRECT_URL is set")
	}

	if err := ping(ctx, directDSN); err != nil {
		return nil, fmt.Errorf("pool fallback: %w", err)
	}

	return openEnt(directDSN)
}

// openEnt opens an ent.Client for the given DSN without pinging (caller already pinged).
func openEnt(dsn string) (*ent.Client, error) {
	client, err := ent.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open pool ent client: %w", err)
	}
	return client, nil
}

// ping opens a temporary sql.DB, runs PingContext, and closes the connection.
// It returns an error if the database is unreachable.
func ping(ctx context.Context, dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed to open postgres connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping postgres: %w", err)
	}

	return nil
}
