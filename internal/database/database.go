package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	_ "github.com/lib/pq"
	"github.com/sanjari-dev/geonera-ingestion/ent"
)

// NewEntClient opens a PostgreSQL connection using DATABASE_URL from the
// environment, verifies connectivity with Ping, then returns both an Ent client
// and the underlying *sql.DB. The caller is responsible for closing both when done.
func NewEntClient(ctx context.Context) (*ent.Client, *sql.DB, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil, nil, fmt.Errorf("DATABASE_URL is not set")
	}

	if err := ping(ctx, dsn); err != nil {
		return nil, nil, err
	}

	return openEnt(dsn)
}

// openEnt opens an ent.Client for the given DSN without pinging (caller already pinged).
// It returns both the ent.Client and the underlying *sql.DB so callers that need
// raw connection access (e.g., advisory locks) do not have to reach through ent internals.
func openEnt(dsn string) (_ *ent.Client, _ *sql.DB, err error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open db: %w", err)
	}
	// Close db if anything after this point fails; on success db ownership
	// transfers to the ent.Client and is released via client.Close().
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()

	// Keep enough idle connections to serve concurrent workers without
	// connection churn. Peak usage: ~12 download workers + ~20 advisory
	// lock sessions + a few regular pipeline transactions ≈ 40 concurrent.
	// MaxIdleConns=15 covers the download workers; advisory locks use
	// dedicated Conn() calls so they are less affected by idle pool size.
	db.SetMaxIdleConns(15)
	db.SetConnMaxLifetime(30 * time.Minute)

	return ent.NewClient(ent.Driver(entsql.OpenDB(dialect.Postgres, db))), db, nil
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
