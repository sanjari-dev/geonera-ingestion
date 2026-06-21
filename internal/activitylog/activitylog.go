package activitylog

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/google/uuid"
	"github.com/sanjari-dev/geonera-ingestion/internal/logger"
)

// TriggerSrc identifies what initiated the job.
type TriggerSrc string

const (
	SrcMQ   TriggerSrc = "MQ"
	SrcHTTP TriggerSrc = "HTTP"
)

// Entry is one row from ingestion.job_activity_logs.
type Entry struct {
	ID          uuid.UUID      `json:"id"`
	TriggerSrc  string         `json:"trigger_src"`
	JobName     string         `json:"job_name"`
	TriggeredAt time.Time      `json:"triggered_at"`
	FinishedAt  *time.Time     `json:"finished_at,omitempty"`
	DurationMs  *int64         `json:"duration_ms,omitempty"`
	TraceID     string         `json:"trace_id,omitempty"`
	Meta        map[string]any `json:"meta,omitempty"`
}

// Logger writes and reads from ingestion.job_activity_logs.
type Logger struct {
	db *sql.DB
}

// New creates a Logger backed by the given database connection.
func New(db *sql.DB) *Logger { return &Logger{db: db} }

// Record inserts an activity log entry synchronously and returns the generated ID.
// The INSERT is intentionally synchronous so that Complete can safely UPDATE the
// same row — the row is guaranteed to exist before the job handler even starts.
func (l *Logger) Record(ctx context.Context, src TriggerSrc, jobName string, meta map[string]any) uuid.UUID {
	id := uuid.New()
	traceID := logger.TraceIDFromContext(ctx)
	metaJSON, _ := json.Marshal(meta)
	now := time.Now().UTC()

	_, err := l.db.ExecContext(ctx, `INSERT INTO ingestion.job_activity_logs (id, trigger_src, job_name, triggered_at, trace_id, meta) VALUES ($1, $2, $3, $4, $5, $6)`, id, string(src), jobName, now, traceID, metaJSON)
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{"trigger_src": src, "job_name": jobName}).Error("activitylog: record")
	}
	return id
}

// retentionDays is the number of days after which completed job activity log
// entries are permanently deleted by Purge.
const retentionDays = 7

// Purge hard-deletes activity log entries whose triggered_at is older than
// retentionDays. Safe to call repeatedly — it is a no-op when nothing qualifies.
// Logged only when at least one row is deleted.
func (l *Logger) Purge(ctx context.Context) {
	result, err := l.db.ExecContext(ctx, `DELETE FROM ingestion.job_activity_logs WHERE triggered_at < NOW() - ($1 || ' days')::INTERVAL`, retentionDays)
	if err != nil {
		logrus.WithError(err).Error("activitylog: purge")
		return
	}
	if n, _ := result.RowsAffected(); n > 0 {
		logrus.WithFields(logrus.Fields{"rows": n, "retention_days": retentionDays}).Info("activitylog: purged entries")
	}
}

// CloseOrphans marks any open activity log entries older than 5 minutes as
// finished (finished_at = NOW(), duration_ms left NULL to signal interrupted).
// Called once at service startup to clean up entries left open by a previous crash.
func (l *Logger) CloseOrphans(ctx context.Context) {
	result, err := l.db.ExecContext(ctx, `UPDATE ingestion.job_activity_logs SET finished_at = NOW() WHERE finished_at IS NULL AND triggered_at < NOW() - INTERVAL '5 minutes'`)
	if err != nil {
		logrus.WithError(err).Error("activitylog: close orphans")
		return
	}
	n, _ := result.RowsAffected()
	if n > 0 {
		logrus.WithField("rows", n).Info("activitylog: closed orphaned entries from previous run")
	}
}

// Cancel deletes the activity log entry entirely. Used when a trigger is
// dropped (lock busy) so that no trace of the skipped trigger appears in
// the activity dashboard.
func (l *Logger) Cancel(id uuid.UUID) {
	go func() {
		_, _ = l.db.ExecContext(context.Background(),
			`DELETE FROM ingestion.job_activity_logs WHERE id = $1`, id,
		)
	}()
}

// Complete updates the activity log entry with the job's finish time and
// duration. It runs fire-and-forget in a goroutine so it never blocks the caller.
// Duration_ms is computed from triggered_at → finished_at in the database to avoid
// clock skew between the INSERT goroutine and this call.
func (l *Logger) Complete(id uuid.UUID) {
	go func() {
		_, err := l.db.ExecContext(context.Background(), `UPDATE ingestion.job_activity_logs SET finished_at = now(), duration_ms = (EXTRACT(EPOCH FROM (now() - triggered_at)) * 1000)::BIGINT WHERE id = $1`, id)
		if err != nil {
			logrus.WithError(err).WithField("state_id", id).Error("activitylog: complete")
		}
	}()
}

// ListResult is the paginated response returned by List.
type ListResult struct {
	Total   int     `json:"total"`
	Entries []Entry `json:"entries"`
}

// List returns paginated activity log entries, optionally filtered by jobName
// and/or triggerSrc. Entries are ordered newest-first.
func (l *Logger) List(ctx context.Context, jobName, triggerSrc string, limit, offset int) (ListResult, error) {
	where := "WHERE 1=1"
	var args []any
	n := 1

	if jobName != "" {
		where += " AND job_name = $" + itoa(n)
		args = append(args, jobName)
		n++
	}
	if triggerSrc != "" {
		where += " AND trigger_src = $" + itoa(n)
		args = append(args, triggerSrc)
		n++
	}

	// COUNT
	var total int
	countArgs := make([]any, len(args))
	copy(countArgs, args)
	if err := l.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM ingestion.job_activity_logs "+where,
		countArgs...,
	).Scan(&total); err != nil {
		return ListResult{}, err
	}

	// Rows
	rowArgs := append(append([]any{}, args...), limit, offset)
	rows, err := l.db.QueryContext(ctx,
		`SELECT id, trigger_src, job_name, triggered_at, finished_at, duration_ms, trace_id, meta
		 FROM ingestion.job_activity_logs
		 `+where+`
		 ORDER BY triggered_at DESC
		 LIMIT $`+itoa(n)+` OFFSET $`+itoa(n+1),
		rowArgs...,
	)
	if err != nil {
		return ListResult{}, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	entries := make([]Entry, 0)
	for rows.Next() {
		var e Entry
		var finishedAt sql.NullTime
		var durationMs sql.NullInt64
		var traceID sql.NullString
		var metaBytes []byte
		if err := rows.Scan(
			&e.ID, &e.TriggerSrc, &e.JobName, &e.TriggeredAt,
			&finishedAt, &durationMs, &traceID, &metaBytes,
		); err != nil {
			return ListResult{}, err
		}
		if finishedAt.Valid {
			e.FinishedAt = &finishedAt.Time
		}
		if durationMs.Valid {
			e.DurationMs = &durationMs.Int64
		}
		if traceID.Valid {
			e.TraceID = traceID.String
		}
		if len(metaBytes) > 0 {
			_ = json.Unmarshal(metaBytes, &e.Meta)
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return ListResult{}, err
	}
	return ListResult{Total: total, Entries: entries}, nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [10]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
