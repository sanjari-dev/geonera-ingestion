package database

import (
	"context"
	"database/sql"
	"fmt"
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

// NewPoolClient opens an ent.Client using DATABASE_POOL_URL (the PgBouncer address).
// All ETL worker goroutines must use this client for short-lived transactions.
// The caller is responsible for closing the client when done.
func NewPoolClient(ctx context.Context) (*ent.Client, error) {
	dsn := os.Getenv("DATABASE_POOL_URL")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_POOL_URL is not set")
	}

	if err := ping(ctx, dsn); err != nil {
		return nil, err
	}

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
