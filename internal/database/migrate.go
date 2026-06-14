package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
)

// activityLogStmts are the DDL statements that create the job_activity_logs table
// and its indexes. Each statement is idempotent (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).
var activityLogStmts = []string{
	`CREATE TABLE IF NOT EXISTS ingestion.job_activity_logs (
		id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
		trigger_src  TEXT        NOT NULL,
		job_name     TEXT        NOT NULL,
		triggered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		trace_id     TEXT,
		meta         JSONB       NOT NULL DEFAULT '{}'::jsonb
	)`,
	// Latency columns — added idempotently so existing deployments are migrated automatically.
	`ALTER TABLE ingestion.job_activity_logs
		ADD COLUMN IF NOT EXISTS finished_at  TIMESTAMPTZ`,
	`ALTER TABLE ingestion.job_activity_logs
		ADD COLUMN IF NOT EXISTS duration_ms  BIGINT`,
	`CREATE INDEX IF NOT EXISTS idx_job_activity_logs_triggered_at
		ON ingestion.job_activity_logs (triggered_at DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_job_activity_logs_job_name
		ON ingestion.job_activity_logs (job_name)`,
	`CREATE INDEX IF NOT EXISTS idx_job_activity_logs_trigger_src
		ON ingestion.job_activity_logs (trigger_src)`,
}

// EnsureActivityLogTable creates ingestion.job_activity_logs and its indexes
// if they do not already exist. Safe to run on every startup (fully idempotent).
func EnsureActivityLogTable(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL is not set")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()

	for _, stmt := range activityLogStmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("activity log DDL: %w", err)
		}
	}
	log.Println("database: activity log table ready")
	return nil
}

// EnsureSchema checks whether the database schema is initialized by querying pg_tables.
// If the master.timeframes table is absent, it reads and executes database/init.sql
// to create all schemas, tables, and seed data automatically.
// This allows the app to self-bootstrap in development without a manual migrated step.
func EnsureSchema(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL is not set")
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
