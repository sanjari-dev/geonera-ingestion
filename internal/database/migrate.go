package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
)

// EnsureSchema checks whether the database schema is initialized by querying pg_tables.
// If the master.timeframes table is absent, it reads and executes database/init.sql
// to create all schemas, tables, and seed data automatically.
// This allows the app to self-bootstrap in development without a manual migrated step.
func EnsureSchema(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_DIRECT_URL")
	if dsn == "" {
		return fmt.Errorf("DATABASE_DIRECT_URL is not set")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	var initialized bool
	err = db.QueryRowContext(ctx,
		`SELECT EXISTS (
			SELECT FROM pg_tables
			WHERE schemaname = 'master' AND tablename = 'timeframes'
		)`).Scan(&initialized)
	if err != nil {
		return fmt.Errorf("check schema: %w", err)
	}

	if initialized {
		return nil
	}

	log.Println("database: schema not found, running init.sql...")

	sqlBytes, err := os.ReadFile("database/init.sql")
	if err != nil {
		return fmt.Errorf("read init.sql: %w", err)
	}

	if _, err := db.ExecContext(ctx, string(sqlBytes)); err != nil {
		return fmt.Errorf("exec init.sql: %w", err)
	}

	log.Println("database: schema initialized successfully")
	return nil
}
