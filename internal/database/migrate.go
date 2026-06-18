package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

// schemaMigration describes a single, ordered schema change.
// Each migration is applied exactly once and recorded in internal.schema_migrations.
//
// Rules:
//   - Never modify or delete an existing entry — that corrupts existing deployments.
//   - To add a schema change: append a new entry with the next version number.
//   - Statements within a migration run inside a single transaction; if any fails,
//     the entire migration rolls back and is NOT recorded.
type schemaMigration struct {
	version int
	desc    string
	stmts   []string
}

// buildMigrations returns the full ordered list of schema migrations.
// Migration 1 is loaded from database/init.sql at call time, so the full DDL
// stays in one authoritative file (readable by DataGrip, IntelliJ, etc.).
func buildMigrations() ([]schemaMigration, error) {
	initSQL, err := os.ReadFile("database/init.sql")
	if err != nil {
		return nil, fmt.Errorf("read init.sql: %w", err)
	}
	return []schemaMigration{
		{
			version: 1,
			desc:    "initial schema — schemas, enums, tables, indexes, seed data",
			stmts:   []string{string(initSQL)},
		},
		{
			version: 2,
			desc:    "activity log table and indexes",
			stmts: []string{
				`CREATE TABLE IF NOT EXISTS ingestion.job_activity_logs (
					id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
					trigger_src  TEXT        NOT NULL,
					job_name     TEXT        NOT NULL,
					triggered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
					trace_id     TEXT,
					meta         JSONB       NOT NULL DEFAULT '{}'::jsonb,
					finished_at  TIMESTAMPTZ,
					duration_ms  BIGINT
				)`,
				// ADD COLUMN guards for installations where the table existed before
				// finished_at / duration_ms were added. No-op on fresh installations.
				`ALTER TABLE ingestion.job_activity_logs
					ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ`,
				`ALTER TABLE ingestion.job_activity_logs
					ADD COLUMN IF NOT EXISTS duration_ms  BIGINT`,
				`CREATE INDEX IF NOT EXISTS idx_job_activity_logs_triggered_at
					ON ingestion.job_activity_logs (triggered_at DESC)`,
				`CREATE INDEX IF NOT EXISTS idx_job_activity_logs_job_name
					ON ingestion.job_activity_logs (job_name)`,
				`CREATE INDEX IF NOT EXISTS idx_job_activity_logs_trigger_src
					ON ingestion.job_activity_logs (trigger_src)`,
			},
		},
		// Add future migrations here — increment version, never edit above entries.
	}, nil
}

// RunMigrations applies all pending schema migrations in version order.
// Each migration runs exactly once; later startups execute a single
// SELECT query against internal.schema_migrations and return immediately
// when everything is up to date.
//
// Baseline handling: if the migration table is empty but master.timeframes
// already exist (deployment predating this system), migration 1 is recorded
// as applied without re-running init.sql (which contains non-idempotent
// CREATE TYPE statements).
func RunMigrations(ctx context.Context) error {
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

	// Bootstrap the tracking table itself. These two statements use IF NOT EXISTS,
	// so they are safe to run on every startup — they are the only DDL that runs
	// unconditionally; everything else is gated by the version check below.
	if _, err := db.ExecContext(ctx, `
		CREATE SCHEMA IF NOT EXISTS internal;
		CREATE TABLE IF NOT EXISTS internal.schema_migrations (
			version    INT         NOT NULL,
			"desc"     TEXT        NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			CONSTRAINT schema_migrations_pkey PRIMARY KEY (version)
		)
	`); err != nil {
		return fmt.Errorf("bootstrap tracking table: %w", err)
	}

	// Baseline: if no migrations are recorded yet but the core schema already
	// exists, this is a deployment that predates this migration system.
	// Mark migration 1 as applied so init.sql is not re-run (CREATE TYPE
	// statements in init.sql have no IF NOT EXISTS guard in PostgreSQL).
	var recorded int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM internal.schema_migrations`,
	).Scan(&recorded); err != nil {
		return fmt.Errorf("count recorded migrations: %w", err)
	}
	if recorded == 0 {
		var schemaExists bool
		if err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT FROM pg_tables
				WHERE schemaname = 'master' AND tablename = 'timeframes'
			)
		`).Scan(&schemaExists); err != nil {
			return fmt.Errorf("check existing schema: %w", err)
		}
		if schemaExists {
			if _, err := db.ExecContext(ctx, `
				INSERT INTO internal.schema_migrations (version, "desc")
				VALUES (1, 'baseline — schema existed before migration tracking was introduced')
				ON CONFLICT DO NOTHING
			`); err != nil {
				return fmt.Errorf("baseline migration 1: %w", err)
			}
			logrus.Info("database: existing schema detected — baseline at migration v1")
		}
	}

	// Determine the highest version already applied.
	var applied int
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM internal.schema_migrations`,
	).Scan(&applied); err != nil {
		return fmt.Errorf("query applied version: %w", err)
	}

	ms, err := buildMigrations()
	if err != nil {
		return err
	}

	pending := 0
	for _, m := range ms {
		if m.version <= applied {
			continue
		}
		pending++
		if err := applyMigration(ctx, db, m); err != nil {
			return fmt.Errorf("migration v%d (%s): %w", m.version, m.desc, err)
		}
	}

	if pending == 0 {
		logrus.Info("database: schema up to date")
	}
	return nil
}

// applyMigration executes all statements for m inside a single transaction.
// If any statement fails, the transaction rolls back, and the migration is NOT
// recorded — the next startup will retry from this version.
func applyMigration(ctx context.Context, db *sql.DB, m schemaMigration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, stmt := range m.stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec stmt: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO internal.schema_migrations (version, "desc") VALUES ($1, $2)`,
		m.version, m.desc,
	); err != nil {
		return fmt.Errorf("record migration: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	logrus.WithFields(logrus.Fields{"version": m.version, "desc": m.desc}).Info("database: migration applied")
	return nil
}
