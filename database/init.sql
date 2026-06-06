-- =============================================================================
-- Geonera Ingestion -- Database Initialization
-- Engine: PostgreSQL 15+
-- Encoding: UTF-8
-- Timezone: UTC  (ensure the server/cluster is configured with SET timezone = 'UTC')
--
-- Usage: Run once against a freshly created, empty database.
--        To re-run safely on an existing database, use the companion
--        drop-all.sql script first, or run each section manually.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 0. SESSION SETTINGS
-- ---------------------------------------------------------------------------
SET client_encoding               = 'UTF8';
SET standard_conforming_strings   = ON;
SET check_function_bodies         = FALSE;
SET client_min_messages           = WARNING;
SET default_transaction_isolation = 'read committed';


-- =============================================================================
-- 1. EXTENSIONS
-- =============================================================================

-- pgcrypto provides gen_random_uuid() used as the default value for all UUID primary keys.
CREATE EXTENSION IF NOT EXISTS pgcrypto;


-- =============================================================================
-- 2. SCHEMAS
-- =============================================================================

-- master: reference / catalogue data (instruments, timeframes).
-- ingestion: operational pipeline data (states, sync_tasks, enum types).
CREATE SCHEMA IF NOT EXISTS master;
CREATE SCHEMA IF NOT EXISTS ingestion;


-- =============================================================================
-- 3. CUSTOM ENUM TYPES  (scoped to the ingestion schema)
-- =============================================================================
-- NOTE: CREATE TYPE has no IF NOT EXISTS clause in PostgreSQL.
--       These statements are intentionally plain top-level DDL so that IDE
--       static analysis (DataGrip, IntelliJ) can resolve the type references
--       in the table definitions below.
-- =============================================================================

-- Defines the category of ingestion job handled by a worker.
CREATE TYPE ingestion.job_type_enum AS ENUM (
    'TICK',
    'CANDLE'
);

-- Full lifecycle of a State / ingestion task.
-- PENDING    - task is queued, not yet picked up by a worker
-- PROCESSED  - worker has claimed the slot and is actively running ETL
-- NOT_FOUND  - data is unavailable at the Dukascopy source (possible release delay)
-- FAILED     - technical error during download or conversion (eligible for retry)
-- COMPLETED  - Parquet file successfully uploaded to R2 (awaiting physical validation)
-- BROKEN     - file on R2 is corrupt or failed physical Parquet validation
-- CONFIRMED  - terminal success: file is 100% valid, or confirmed as a permanent
--              holiday after NotFoundStreak >= 3 with a zero-row Parquet committed
-- ABANDONED  - terminal failure: retry limit (>= 5) reached, requires manual intervention
CREATE TYPE ingestion.status_enum AS ENUM (
    'PENDING',
    'PROCESSED',
    'NOT_FOUND',
    'FAILED',
    'COMPLETED',
    'BROKEN',
    'CONFIRMED',
    'ABANDONED'
);

-- Simplified two-state lifecycle used exclusively by sync_tasks (Outbox Pattern).
CREATE TYPE ingestion.sync_status_enum AS ENUM (
    'PENDING',
    'PROCESSED'
);


-- =============================================================================
-- 4. TABLE: master.instruments
-- =============================================================================
-- Catalogue of all financial instruments (currency pairs, CFDs, commodities, etc.)
-- monitored by the Dukascopy ingestion pipeline.
-- =============================================================================

CREATE TABLE IF NOT EXISTS master.instruments (
    id          UUID    NOT NULL DEFAULT gen_random_uuid(),
    name        VARCHAR NOT NULL,
    description TEXT    NOT NULL,
    asset_class VARCHAR NOT NULL,

    -- Whether the instrument is currently active in the ingestion pipeline.
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,

    -- Divisor applied to the raw integer price from the Dukascopy BI5 feed
    -- (e.g., 100,000 for 5-decimal FX pairs such as EURUSD).
    divider     INT     NOT NULL CHECK (divider > 0),

    -- The earliest date for which historical data is available at Dukascopy.
    -- Always stored in UTC. All local timezone inputs must be converted to UTC
    -- before persisting to avoid off-by-hours errors in daily gap detection.
    start_date  TIMESTAMPTZ NOT NULL,

    -- Mutex flag: set to TRUE automatically when a per-instrument data
    -- inconsistency is detected. While TRUE, the regular pipeline skips this
    -- instrument and leaves its states as PENDING until resolved manually.
    is_pause    BOOLEAN NOT NULL DEFAULT FALSE,

    CONSTRAINT instruments_pkey     PRIMARY KEY (id),
    CONSTRAINT instruments_name_key UNIQUE (name)
);

COMMENT ON TABLE  master.instruments            IS 'Catalogue of financial instruments monitored by the Dukascopy ingestion pipeline.';
COMMENT ON COLUMN master.instruments.divider    IS 'Raw price divisor applied to BI5 integer values (e.g., 100,000 for 5-decimal FX pairs).';
COMMENT ON COLUMN master.instruments.start_date IS 'Earliest historical data availability date at Dukascopy (UTC).';
COMMENT ON COLUMN master.instruments.is_pause   IS 'Mutex flag: TRUE = pipeline paused for this instrument pending manual resolution.';


-- =============================================================================
-- 5. TABLE: master.timeframes
-- =============================================================================
-- Fixed catalogue of 19 standard timeframes supported by the CANDLE pipeline.
-- This table is immutable after initial seeding -- rows must not be added or removed.
-- Valid names: m1, m2, m3, m4, m5, m6, m10, m12, m15, m20, m30,
--              h1, h2, h3, h4, h6, h8, h12, d1.
-- =============================================================================

CREATE TABLE IF NOT EXISTS master.timeframes (
    id        UUID    NOT NULL DEFAULT gen_random_uuid(),
    name      VARCHAR NOT NULL,
    -- Timeframe duration in minutes (e.g., m1 = 1, h1 = 60, d1 = 1440).
    minutes   INT     NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,

    CONSTRAINT timeframes_pkey     PRIMARY KEY (id),
    CONSTRAINT timeframes_name_key UNIQUE (name)
);

COMMENT ON TABLE  master.timeframes         IS 'Fixed catalogue of 19 standard timeframes for the CANDLE aggregation pipeline.';
COMMENT ON COLUMN master.timeframes.minutes IS 'Duration in minutes (m1=1, m5=5, h1=60, d1=1440).';


-- =============================================================================
-- 6. TABLE: ingestion.states
-- =============================================================================
-- Core state-management table of the ingestion pipeline. Each row represents
-- one time slot whose data must be fetched from Dukascopy for a given
-- instrument and job type (TICK or CANDLE).
--
-- The composite unique constraint (instrument_id, timestamp, job_type) is the
-- idempotency guarantee: no two workers can claim the same slot simultaneously.
-- All numeric counters (retry_count, not_found_streak, resolved_tick_count) must
-- be mutated atomically at the SQL level -- never via read-modify-write in Go.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ingestion.states (
    id                  UUID                        NOT NULL DEFAULT gen_random_uuid(),
    instrument_id       UUID                        NOT NULL,
    job_type            ingestion.job_type_enum     NOT NULL,

    -- Start of the time slot represented by this State (UTC).
    -- For TICK jobs this is the top of the hour; for CANDLE jobs it is 00:00:00 UTC.
    "timestamp"         TIMESTAMPTZ                 NOT NULL,

    status              ingestion.status_enum       NOT NULL,

    -- Holds the status value immediately before the most recent transition.
    -- Must be shifted atomically on every status update (no separate round-trip).
    -- Used to detect demotions (e.g., CONFIRMED -> BROKEN) inside the Outbox Worker.
    previous_status     ingestion.status_enum,

    -- Explicit holiday marker. Set to TRUE only when the system intentionally
    -- commits a Zero-Row Parquet file for a permanently empty market slot.
    is_holiday          BOOLEAN                     NOT NULL DEFAULT FALSE,

    -- Daily counter for CANDLE jobs: tracks how many TICK states for the same
    -- instrument/day have reached CONFIRMED. Aggregation triggers when this value = 24.
    resolved_tick_count INT                         NOT NULL DEFAULT 0,

    -- Incremented atomically (+1) on each FAILED or BROKEN retry attempt.
    -- Transitions to ABANDONED when this reaches the maximum threshold (>= 5).
    retry_count         INT                         NOT NULL DEFAULT 0,

    -- Incremented atomically (+1) only when the Dukascopy API returns 404.
    -- Must be reset to 0 if data is subsequently found on a re-check.
    -- Triggers Zero-Row Parquet commitment when this reaches >= 3.
    not_found_streak    INT                         NOT NULL DEFAULT 0,

    -- Soft-delete flag supporting the Mark phase of 2-Phase Commit pruning.
    -- Rows marked TRUE are invisible to all ETL workers but retained for audit.
    is_deleted          BOOLEAN                     NOT NULL DEFAULT FALSE,

    -- Timestamp of the last status transition. Maintained by the application, not a trigger.
    updated_at          TIMESTAMPTZ                 NOT NULL,

    CONSTRAINT states_pkey PRIMARY KEY (id),

    CONSTRAINT states_instrument_id_fkey
        FOREIGN KEY (instrument_id)
        REFERENCES master.instruments (id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT  -- Prevent accidental deletion of instruments with active states.
);

-- Composite unique index -- the idempotency key for the entire pipeline.
-- Also leveraged by the advisory-lock handler to detect duplicate INSERTs before they happen.
CREATE UNIQUE INDEX IF NOT EXISTS uidx_states_instrument_timestamp_jobtype
    ON ingestion.states (instrument_id, "timestamp", job_type);

-- Partial index for the worker claim queue: excludes soft-deleted rows,
-- dramatically reducing index size on high-volume tables.
CREATE INDEX IF NOT EXISTS idx_states_status
    ON ingestion.states (status)
    WHERE is_deleted = FALSE;

-- Supporting index: efficiently fetch all time slots for a single instrument.
CREATE INDEX IF NOT EXISTS idx_states_instrument_id
    ON ingestion.states (instrument_id);

COMMENT ON TABLE  ingestion.states                     IS 'Ingestion time slots per instrument and job type. One row = one unit of work for a worker.';
COMMENT ON COLUMN ingestion.states."timestamp"         IS 'Start of the time slot (UTC). Part of the composite unique key.';
COMMENT ON COLUMN ingestion.states.previous_status     IS 'Status before the most recent transition. Used to detect demotions such as CONFIRMED to BROKEN.';
COMMENT ON COLUMN ingestion.states.resolved_tick_count IS 'Count of CONFIRMED TICK states for the same instrument and day. CANDLE aggregation triggers at 24.';
COMMENT ON COLUMN ingestion.states.not_found_streak    IS 'Consecutive NOT_FOUND count. Resets to 0 when data is found. Triggers Zero-Row Parquet at 3 or above.';
COMMENT ON COLUMN ingestion.states.retry_count         IS 'Cumulative retry count. State transitions to ABANDONED at 5 or above.';
COMMENT ON COLUMN ingestion.states.is_deleted          IS 'Soft-delete mark for the Mark phase of 2-Phase Commit pruning.';
COMMENT ON COLUMN ingestion.states.updated_at          IS 'Timestamp of the last status transition, maintained by the application.';


-- =============================================================================
-- 7. TABLE: ingestion.sync_tasks
-- =============================================================================
-- Transactional Outbox table (Outbox Pattern). When a TICK state reaches a
-- terminal status (CONFIRMED) or is demoted (CONFIRMED -> BROKEN), an event row
-- is upserted here atomically within the same database transaction as the
-- status change. A dedicated Outbox Worker consumes PENDING rows and triggers
-- a recomputation of resolved_tick_count on the corresponding CANDLE state.
--
-- The composite unique constraint (instrument_id, target_date) collapses rapid
-- successive events for the same instrument/day into a single row, preventing
-- count drift from concurrent terminal transitions.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ingestion.sync_tasks (
    id            UUID                        NOT NULL DEFAULT gen_random_uuid(),
    instrument_id UUID                        NOT NULL,

    -- The calendar day (00:00:00 UTC) of the TICK slot that triggered this event.
    target_date   TIMESTAMPTZ                 NOT NULL,

    status        ingestion.sync_status_enum  NOT NULL DEFAULT 'PENDING',
    created_at    TIMESTAMPTZ                 NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT sync_tasks_pkey PRIMARY KEY (id),

    CONSTRAINT sync_tasks_instrument_id_fkey
        FOREIGN KEY (instrument_id)
        REFERENCES master.instruments (id)
        ON UPDATE CASCADE
        ON DELETE CASCADE  -- If the instrument is removed, clean up its outbox events too.
);

-- Composite unique index -- at most one pending sync event per instrument per day.
-- The application ON CONFLICT clause upserts the status back to PENDING, guaranteeing
-- a recomputation even when multiple terminal transitions occur in rapid succession.
CREATE UNIQUE INDEX IF NOT EXISTS uidx_sync_tasks_instrument_target_date
    ON ingestion.sync_tasks (instrument_id, target_date);

-- Supporting index: Outbox Worker scans exclusively for PENDING tasks.
CREATE INDEX IF NOT EXISTS idx_sync_tasks_status
    ON ingestion.sync_tasks (status);

COMMENT ON TABLE  ingestion.sync_tasks             IS 'Transactional Outbox events for recomputing resolved_tick_count on CANDLE states. Consumed by the Outbox Worker.';
COMMENT ON COLUMN ingestion.sync_tasks.target_date IS 'Calendar day (00:00:00 UTC) of the TICK slot that triggered this sync event.';


-- =============================================================================
-- 8. SEED DATA
-- =============================================================================
-- Inserts the fixed catalogue of 19 standard timeframes.
-- ON CONFLICT DO NOTHING makes this block safe to re-run on an existing database.
-- =============================================================================

INSERT INTO master.timeframes (name, minutes, is_active) VALUES
    ('m1',   1,    TRUE),
    ('m2',   2,    TRUE),
    ('m3',   3,    TRUE),
    ('m4',   4,    TRUE),
    ('m5',   5,    TRUE),
    ('m6',   6,    TRUE),
    ('m10',  10,   TRUE),
    ('m12',  12,   TRUE),
    ('m15',  15,   TRUE),
    ('m20',  20,   TRUE),
    ('m30',  30,   TRUE),
    ('h1',   60,   TRUE),
    ('h2',   120,  TRUE),
    ('h3',   180,  TRUE),
    ('h4',   240,  TRUE),
    ('h6',   360,  TRUE),
    ('h8',   480,  TRUE),
    ('h12',  720,  TRUE),
    ('d1',   1440, TRUE)
ON CONFLICT (name) DO NOTHING;


-- =============================================================================
-- 9. SEED DATA — Instruments
-- =============================================================================
-- Default instruments for the ingestion pipeline.
-- is_active = FALSE: instruments are inactive by default and must be explicitly
-- enabled (along with start_date correction) before the pipeline processes them.
-- start_date defaults to 2003-01-01 for all instruments.
-- ON CONFLICT DO NOTHING makes this block safe to re-run on an existing database.
-- =============================================================================

INSERT INTO master.instruments (name, description, asset_class, is_active, divider, start_date) VALUES
    ('XAUUSD', 'Gold vs US Dollar',                    'Commodity', FALSE, 3, '2003-01-01 00:00:00+00'),
    ('AUDJPY', 'Australian Dollar vs Japanese Yen',    'Forex',     FALSE, 3, '2003-01-01 00:00:00+00'),
    ('CHFJPY', 'Swiss Franc vs Japanese Yen',          'Forex',     FALSE, 3, '2003-01-01 00:00:00+00'),
    ('CADJPY', 'Canadian Dollar vs Japanese Yen',      'Forex',     FALSE, 3, '2003-01-01 00:00:00+00'),
    ('GBPJPY', 'British Pound vs Japanese Yen',        'Forex',     FALSE, 3, '2003-01-01 00:00:00+00'),
    ('EURJPY', 'Euro vs Japanese Yen',                 'Forex',     FALSE, 3, '2003-01-01 00:00:00+00'),
    ('NZDJPY', 'New Zealand Dollar vs Japanese Yen',   'Forex',     FALSE, 3, '2003-01-01 00:00:00+00'),
    ('USDJPY', 'US Dollar vs Japanese Yen',            'Forex',     FALSE, 3, '2003-01-01 00:00:00+00'),
    ('GBPUSD', 'British Pound vs US Dollar',           'Forex',     FALSE, 5, '2003-01-01 00:00:00+00'),
    ('EURUSD', 'Euro vs US Dollar',                    'Forex',     FALSE, 5, '2003-01-01 00:00:00+00'),
    ('USDCHF', 'US Dollar vs Swiss Franc',             'Forex',     FALSE, 5, '2003-01-01 00:00:00+00'),
    ('USDCAD', 'US Dollar vs Canadian Dollar',         'Forex',     FALSE, 5, '2003-01-01 00:00:00+00'),
    ('AUDUSD', 'Australian Dollar vs US Dollar',       'Forex',     FALSE, 5, '2003-01-01 00:00:00+00'),
    ('NZDUSD', 'New Zealand Dollar vs US Dollar',      'Forex',     FALSE, 5, '2003-01-01 00:00:00+00'),
    ('EURGBP', 'Euro vs British Pound',                'Forex',     FALSE, 5, '2003-01-01 00:00:00+00'),
    ('EURNZD', 'Euro vs New Zealand Dollar',           'Forex',     FALSE, 5, '2003-01-01 00:00:00+00')
ON CONFLICT (name) DO NOTHING;


-- =============================================================================
-- END OF INIT SCRIPT
-- =============================================================================
