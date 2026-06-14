package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	_ "github.com/lib/pq"
	"github.com/sanjari-dev/geonera-ingestion/ent"
)

// NewEntClient opens a PostgreSQL connection using DATABASE_URL from the
// environment, verifies connectivity with Ping, then returns an Ent client.
// The caller is responsible for closing the client when done.
func NewEntClient(ctx context.Context) (*ent.Client, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_URL is not set")
	}

	if err := ping(ctx, dsn); err != nil {
		return nil, err
	}

	return openEnt(dsn)
}

// openEnt opens an ent.Client for the given DSN without pinging (caller already pinged).
func openEnt(dsn string) (_ *ent.Client, err error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	// Close db if anything after this point fails; on success db ownership
	// transfers to the ent.Client and is released via client.Close().
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()

	return ent.NewClient(ent.Driver(entsql.OpenDB(dialect.Postgres, db))), nil
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
