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

	// Use a short-lived sql.DB solely to verify connectivity before committing
	// to a full Ent client. The connection is closed regardless of the outcome.
	if err := ping(ctx, dsn); err != nil {
		return nil, err
	}

	// ent.Open manages its own internal driver and connection pool.
	// No intermediate closeable resource is exposed here.
	client, err := ent.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open ent client: %w", err)
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
