package worker

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"entgo.io/ent/dialect/sql"
	"github.com/google/uuid"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/instrument"
	"github.com/sanjari-dev/geonera-ingestion/ent/predicate"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/ent/synctask"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/dukascopy"
	ilogger "github.com/sanjari-dev/geonera-ingestion/internal/logger"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
	"github.com/sanjari-dev/geonera-ingestion/internal/tickparquet"
)

const (
	// notFoundThreshold is the consecutive-404 count that triggers a Zero-Row Parquet commit.
	notFoundThreshold = 3
	// dukascopyMaxRPS is the global maximum requests-per-second sent to Dukascopy.
	// Aligned with dukascopyBurst (= concurrency cap) so every semaphore slot can
	// fire once per second in a steady state.  This prevents 503 thundering-herds
	// without slowing down the pipeline: a fresh worker saturates all 12 slots in
	// the first second, then sustains 12 downloads/s.
	dukascopyMaxRPS = 12

	// dukascopyBurst is the token-bucket burst capacity — equals the concurrency cap,
	// so the burst and sustained rate are identical (12 per second).
	dukascopyBurst = dukascopyMaxRPS

	// backfillExclusionHours is the "Zona Eksklusif" boundary (Mapping
	// State.md §2): Backfill only ever claims rows timestamped at or before
	// (current hour − this many hours) — i.e., T-3 and older — leaving the
	// most recent hours to the T-0/T-1/T-2 Regular pipeline.
	backfillExclusionHours = 3
)

// backfillMasterClaimLimit is the batch size for the Backfill "Master Bulk-Claim":
// a single FOR UPDATE SKIP LOCKED query locks up to this many rows per cycle.
// Default 120; overridable at startup via BACKFILL_CLAIM_LIMIT env var (must be > 0).
// There is intentionally NO separate goroutine-pool cap on top of this: every
// claimed row is dispatched immediately, and the real bottlenecks —
// the download gate (dukascopyBurst workers) and tickProcessSem (runtime.NumCPU())
// — already bound concurrency. This value controls in-flight PROCESSED-row count
// and therefore goroutine/memory growth.
var backfillMasterClaimLimit = 120

// instrName returns the instrument name from a state row's edge, falling back
// to the UUID string when the edge was not eagerly loaded (e.g., queries missing
// WithInstrument). Safe to call on any *ent.State.
func instrName(row *ent.State) string {
	if row.Edges.Instrument != nil {
		return row.Edges.Instrument.Name
	}
	return row.InstrumentID.String()
}

// prevStatusStr formats a nullable PreviousStatus pointer for log output.
func prevStatusStr(p *state.PreviousStatus) string {
	if p == nil {
		return "nil"
	}
	return string(*p)
}

// tickDownloadRateGate is a token-bucket rate limiter for Dukascopy downloads.
// A background goroutine (started by InitDownloadRateLimiter) refills it at
// dukascopyMaxRPS tokens/second. Each download worker consumes one token before
// sending the HTTP request, capping the global request rate even when responses
// return quickly (e.g., 503 errors).
var tickDownloadRateGate = make(chan struct{}, dukascopyBurst)

// InitDownloadRateLimiter pre-fills the token bucket and starts the background
// goroutine that replenishes it at dukascopyMaxRPS tokens/second.
// Must be called once at startup, before any worker goroutine is dispatched.
func InitDownloadRateLimiter() {
	// Pre-fill bucket so the first burst of dukascopyBurst downloads starts immediately.
	for i := 0; i < dukascopyBurst; i++ {
		select {
		case tickDownloadRateGate <- struct{}{}:
		default:
		}
	}
	interval := time.Second / dukascopyMaxRPS
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			select {
			case tickDownloadRateGate <- struct{}{}:
			default: // bucket full — drop token (prevents stale burst after idle)
			}
		}
	}()
	// Start fixed pool of download worker goroutines.
	startDownloadWorkers()

	ilogger.T(context.Background()).WithFields(logrus.Fields{
		"max_rps":  dukascopyMaxRPS,
		"burst":    dukascopyBurst,
		"interval": interval,
	}).Info("worker: download rate limiter started")
}

// tickProcessSem limits concurrent Parquet convert+upload goroutines to
// runtime.NumCPU() so the convert stage (CPU-bound) saturates all available
// cores without oversubscribing. Initialized once at package load time.
// Shared across all tick phases.
var tickProcessSem = make(chan struct{}, runtime.NumCPU())

// regularTimeout is the maximum wall-clock time allowed for one REGULAR cycle
// (T-0 + T-1 + T-2). A cycle that exceeds this is considered abnormal — likely
// a hung R2 operation or an undetected stale connection — and is forcibly
// canceled so it does not block the next trigger window.
const regularTimeout = 50 * time.Minute

// backfillTimeout is the maximum wall-clock time allowed for one BACKFILL
// master-claim batch. Without this, goroutines that block waiting for a slot in
// the download queue have no escape valve: as triggers accumulate (each adding
// up to backfillMasterClaimLimit goroutines), goroutines pile up indefinitely
// because context.Background() (passed by the MQ consumer) has no deadline.
// 9 minutes gives a full batch of 120 rows enough time to complete at the
// sustained download rate (12 req/s, ~2.4 completed/s under Dukascopy timeouts)
// while still bounding goroutine lifetime to a predictable window.
const backfillTimeout = 9 * time.Minute

// RunTickParentHandler is the single entry point for all tick-ingestion work.
// Mode must be either "REGULAR" or "BACKFILL".
func RunTickParentHandler(ctx context.Context, mode string, d *dal.DAL, onStarted func()) bool {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "RunTickParentHandler", "mode": mode}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "RunTickParentHandler").Trace("fn_exit")

	if onStarted != nil {
		onStarted()
	}
	ilogger.T(ctx).WithField("mode", mode).Info("ticks: handler start")

	runCtx := ctx
	var cancel context.CancelFunc
	switch mode {
	case "REGULAR":
		ilogger.T(ctx).WithField("timeout", regularTimeout).Trace("branch: REGULAR timeout set")
		runCtx, cancel = context.WithTimeout(ctx, regularTimeout)
		defer cancel()
	case "BACKFILL":
		ilogger.T(ctx).WithField("timeout", backfillTimeout).Trace("branch: BACKFILL timeout set")
		runCtx, cancel = context.WithTimeout(ctx, backfillTimeout)
		defer cancel()
	}

	var wg sync.WaitGroup
	switch mode {
	case "REGULAR":
		ilogger.T(ctx).Trace("branch: launching T0+T1+T2 goroutines")
		wg.Add(3)
		go runT0Phase(runCtx, d, &wg)
		go runT1Phase(runCtx, d, &wg)
		go runT2Phase(runCtx, d, &wg)
	case "BACKFILL":
		ilogger.T(ctx).Trace("branch: launching backfill master loop goroutine")
		wg.Add(1)
		go runBackfillMasterLoop(runCtx, d, &wg)
	default:
		ilogger.T(ctx).WithField("mode", mode).Warn("ticks: unknown mode")
	}

	ilogger.T(ctx).WithField("mode", mode).Trace("before wg.Wait")
	wg.Wait()
	ilogger.T(ctx).WithField("mode", mode).Trace("after wg.Wait")

	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		switch mode {
		case "REGULAR":
			ilogger.T(ctx).WithField("timeout", regularTimeout).Warn("ticks/REGULAR: deadline exceeded — cycle was abnormal, all phases canceled")
		case "BACKFILL":
			ilogger.T(ctx).WithField("timeout", backfillTimeout).Warn("ticks/BACKFILL: deadline exceeded — batch goroutines canceled, goroutine leak prevented")
		}
	}

	ilogger.T(ctx).WithField("mode", mode).Info("ticks: handler done")
	return true
}

// ── Loop implementations ──────────────────────────────────────────────────────

func tickActiveStateRows(q *ent.StateQuery, filters ...predicate.State) *ent.StateQuery {
	baseFilters := []predicate.State{
		state.JobTypeEQ(state.JobTypeTICK),
		state.HasInstrumentWith(
			instrument.IsPauseEQ(false),
			instrument.IsActiveEQ(true),
		),
		state.IsDeletedEQ(false),
	}
	return q.Where(append(baseFilters, filters...)...)
}

func orderStateStatuses(statuses ...state.Status) func(*sql.Selector) {
	return func(s *sql.Selector) {
		s.OrderExprFunc(func(b *sql.Builder) {
			b.WriteString("CASE status ")
			for idx, status := range statuses {
				b.WriteString("WHEN '")
				b.WriteString(string(status))
				b.WriteString(fmt.Sprintf("' THEN %d ", idx+1))
			}
			b.WriteString(fmt.Sprintf("ELSE %d END", len(statuses)+1))
		})
	}
}

// ── ETL execution ─────────────────────────────────────────────────────────────

// executeIngestionETL runs the download → convert → upload pipeline for a row
// that has already been claimed (status = PROCESSED). Updates the row's status
// to COMPLETED, FAILED, or NOT_FOUND.
//
// onNotFound controls how the streak is recorded on a 404 response:
//   - T-0 passes handleNotFoundSimple (SetNotFoundStreak=1, always first attempt)
//   - Backfill Group 1 passes handleNotFoundIncrement (AddNotFoundStreak+1, streak may be >0)
//
// highPriority routes the download to regularDownloadQueue (true) or
// backfillDownloadQueue (false); see RequestDownload for the priority contract.
func executeIngestionETL(ctx context.Context, d *dal.DAL, row *ent.State, onNotFound func(context.Context, *dal.DAL, *ent.State), highPriority bool) {
	inst := instrName(row)
	ts := row.Timestamp.Format(time.RFC3339)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeIngestionETL", "instrument": inst, "ts": ts, "high_priority": highPriority}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "executeIngestionETL").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{
		"state_id": row.ID, "retry_count": row.RetryCount, "streak": row.NotFoundStreak, "status": row.Status,
	}).Debug("ticks/ETL: row vars")
	start := time.Now()
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Info("ticks/ETL: start")

	// Phase 1: Download BI5 — submitted to the download gate worker pool.
	// The gate manages concurrency (dukascopyBurst workers) and rate limiting
	// (dukascopyMaxRPS tokens/s). The call respects ctx cancellation at every
	// blocking point: enqueue, rate-gate wait, and result wait.
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeIngestionETL", "instrument": inst}).Trace("before call: RequestDownload")
	dl := RequestDownload(ctx, row, highPriority)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeIngestionETL", "status": dl.Status}).Trace("after call: RequestDownload")

	switch dl.Status {
	case DownloadNotFound:
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Trace("branch: download_not_found")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "elapsed": time.Since(start).Round(time.Millisecond)}).Info("ticks/ETL: download NOT_FOUND")
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeIngestionETL"}).Trace("before call: onNotFound")
		onNotFound(ctx, d, row)
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeIngestionETL"}).Trace("after call: onNotFound")
		return
	case DownloadOK:
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "bytes": len(dl.Data)}).Trace("branch: download_ok")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "bytes": len(dl.Data), "elapsed": time.Since(start).Round(time.Millisecond)}).Info("ticks/ETL: download OK")
	default: // DownloadRateLimited, DownloadError, or ctx canceled
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Trace("branch: download_error")
		ilogger.T(ctx).WithError(dl.Err).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "elapsed": time.Since(start).Round(time.Millisecond)}).Error("ticks/ETL: download ERROR → FAILED")
		ilogger.T(ctx).WithField("fn", "executeIngestionETL").Trace("before call: updateStateFailed")
		updateStateFailed(ctx, d, row)
		ilogger.T(ctx).WithField("fn", "executeIngestionETL").Trace("after call: updateStateFailed")
		return
	}

	// Data was found: reset NotFoundStreak to 0 if it was previously non-zero.
	if row.NotFoundStreak > 0 {
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeIngestionETL", "streak": row.NotFoundStreak}).Trace("before call: resetNotFoundStreak")
		resetNotFoundStreak(ctx, d, row, "reset streak")
		ilogger.T(ctx).WithField("fn", "executeIngestionETL").Trace("after call: resetNotFoundStreak")
	}

	// Phase 2: Convert + Upload — semaphore capped at runtime.NumCPU().
	ilogger.T(ctx).WithField("fn", "executeIngestionETL").Trace("before call: executeConvertUpload")
	executeConvertUpload(ctx, d, row, dl.Data)
	ilogger.T(ctx).WithField("fn", "executeIngestionETL").Trace("after call: executeConvertUpload")
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "elapsed": time.Since(start).Round(time.Millisecond)}).Info("ticks/ETL: pipeline done")
}

// executeNotFoundRecheck re-checks the Dukascopy API for a claimed NOT_FOUND row.
// Used by T-1 recovery (executeRecoveryAction) and Backfill Layer D.
//
// Data found → SetNotFoundStreak=0, Status=PENDING (ETL on the next cycle).
// Still 404 → AddNotFoundStreak(+1, streak=2), IsHoliday=true, upload Zero-Row Parquet, Status=NOT_FOUND.
// Other error → FAILED (handled by backfill-reset on retry).
//
// highPriority routes the download to regularDownloadQueue (true) or
// backfillDownloadQueue (false); see RequestDownload for the priority contract.
func executeNotFoundRecheck(ctx context.Context, d *dal.DAL, row *ent.State, highPriority bool) {
	inst := instrName(row)
	ts := row.Timestamp.Format(time.RFC3339)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeNotFoundRecheck", "instrument": inst, "ts": ts, "streak": row.NotFoundStreak}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "executeNotFoundRecheck").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "streak": row.NotFoundStreak}).Info("ticks/recheck: start")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeNotFoundRecheck", "instrument": inst}).Trace("before call: RequestDownload")
	dl := RequestDownload(ctx, row, highPriority)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeNotFoundRecheck", "status": dl.Status}).Trace("after call: RequestDownload")

	if dl.Status != DownloadOK {
		if dl.Status == DownloadNotFound {
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Trace("branch: recheck_still_not_found")
			// Still 404: increment streak, mark holiday, upload Zero-Row Parquet.
			// DB commit happens before R2 upload, so IsHoliday flag persists even on
			// upload failure. If the upload fails, the row is set to FAILED so that
			// T-1/Backfill can retry the full pipeline on the next cycle.
			newStreak := row.NotFoundStreak + 1
			ilogger.T(ctx).WithFields(logrus.Fields{
				"old_streak": row.NotFoundStreak, "new_streak": newStreak,
			}).Debug("ticks/recheck: streak vars")
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "instrument": inst}).Trace("before query")
			if dbErr := d.Execute(ctx, func(tx *ent.Tx) error {
				_, e := tx.State.UpdateOneID(row.ID).
					AddNotFoundStreak(1).
					SetIsHoliday(true).
					SetPreviousStatus(state.PreviousStatusPROCESSED).
					SetStatus(state.StatusNOT_FOUND).
					SetUpdatedAt(time.Now().UTC()).
					Save(ctx)
				return e
			}); dbErr != nil {
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
				ilogger.T(ctx).WithError(dbErr).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Error("ticks/recheck: DB update FAILED")
				return
			}
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false, "new_streak": newStreak}).Trace("after query")
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "new_streak": newStreak}).Info("ticks/recheck: still NOT_FOUND → uploading zero-row")
			zeroRow := buildZeroRowParquet()
			ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeNotFoundRecheck", "instrument": inst}).Trace("before r2 upload: zero-row")
			upErr := uploadToR2(ctx, row, zeroRow)
			ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeNotFoundRecheck", "err": upErr != nil}).Trace("after r2 upload: zero-row")
			if upErr != nil {
				ilogger.T(ctx).WithError(upErr).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Error("ticks/recheck: zero-row upload FAILED → FAILED")
				updateStateFailed(ctx, d, row)
			} else {
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Info("ticks/recheck: zero-row uploaded → NOT_FOUND")
			}
		} else {
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Trace("branch: recheck_download_error")
			ilogger.T(ctx).WithError(dl.Err).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Error("ticks/recheck: download ERROR → FAILED")
			ilogger.T(ctx).WithField("fn", "executeNotFoundRecheck").Trace("before call: updateStateFailed")
			updateStateFailed(ctx, d, row)
			ilogger.T(ctx).WithField("fn", "executeNotFoundRecheck").Trace("after call: updateStateFailed")
		}
		return
	}

	// Data is available again: reset streak to 0, set PENDING.
	// The row will be picked up for a full ETL pass on the next cycle.
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Trace("branch: recheck_data_found")
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Info("ticks/recheck: data found → streak=0 PENDING")
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "instrument": inst}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			SetNotFoundStreak(0).
			SetPreviousStatus(state.PreviousStatusNOT_FOUND).
			SetStatus(state.StatusPENDING).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Error("ticks/recheck: reset to PENDING FAILED")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false}).Trace("after query")
}

// executeT2Action implements the T-2 cross-validation pipeline.
// Both COMPLETED and NOT_FOUND rows are re-downloaded from Dukascopy for verification.
//
//	COMPLETED + 2xx → convert + upload (overwrite) + validate → CONFIRMED or BROKEN.
//	NOT_FOUND + 2xx → SetNotFoundStreak=0, Status=PENDING (ETL deferred to next cycle).
//	NOT_FOUND + 404 → AddNotFoundStreak(+1); streak≥3 → executeValidation → CONFIRMED.
//	COMPLETED + 404 → BROKEN (data was present at T-0 but is now missing — anomaly).
//	Non-404 error → FAILED.
//
// highPriority routes the download to regularDownloadQueue (true) or
// backfillDownloadQueue (false); see RequestDownload for the priority contract.
func executeT2Action(ctx context.Context, d *dal.DAL, row *ent.State, preclaimPrev *state.PreviousStatus, highPriority bool) {
	inst := instrName(row)
	ts := row.Timestamp.Format(time.RFC3339)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeT2Action", "instrument": inst, "ts": ts, "prev_status": prevStatusStr(preclaimPrev), "streak": row.NotFoundStreak}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{
		"instrument":  inst,
		"ts":          ts,
		"prev_status": prevStatusStr(row.PreviousStatus),
		"streak":      row.NotFoundStreak,
	}).Info("ticks/T2: action start")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeT2Action", "instrument": inst}).Trace("before call: RequestDownload")
	dl := RequestDownload(ctx, row, highPriority)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeT2Action", "status": dl.Status}).Trace("after call: RequestDownload")
	wasNotFound := row.PreviousStatus != nil && *row.PreviousStatus == state.PreviousStatusNOT_FOUND
	ilogger.T(ctx).WithFields(logrus.Fields{
		"was_not_found": wasNotFound, "streak": row.NotFoundStreak,
		"prev_status": prevStatusStr(preclaimPrev), "dl_status": dl.Status,
	}).Debug("ticks/T2: action vars")

	if dl.Status != DownloadOK {
		if dl.Status == DownloadNotFound {
			if wasNotFound {
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "streak": row.NotFoundStreak}).Trace("branch: T2_not_found+was_not_found")
				// NOT_FOUND + 404: increment streak.
				// At ≥ notFoundThreshold: validate the Zero-Row Parquet that T-1 already
				// uploaded to R2 → CONFIRMED (same validation path as COMPLETED+2xx).
				// Below threshold: set NOT_FOUND and wait for the next cycle.
				newStreak := row.NotFoundStreak + 1
				newRetryCount := row.RetryCount - 1
				if newRetryCount < 0 {
					newRetryCount = 0
				}
				ilogger.T(ctx).WithFields(logrus.Fields{
					"old_streak": row.NotFoundStreak, "new_streak": newStreak,
					"retry_count_before": row.RetryCount, "new_retry_count": newRetryCount,
					"threshold": notFoundThreshold,
				}).Debug("ticks/T2: streak vars")
				if newStreak >= notFoundThreshold {
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "streak": newStreak, "threshold": notFoundThreshold}).Trace("branch: T2_streak_ge_threshold")
					ilogger.T(ctx).WithFields(logrus.Fields{
						"instrument": inst,
						"ts":         ts,
						"streak":     newStreak,
						"threshold":  notFoundThreshold,
					}).Info("ticks/T2: NOT_FOUND streak >= threshold → validate zero-row")
					ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save"}).Trace("before query")
					if dbErr := d.Execute(ctx, func(tx *ent.Tx) error {
						_, e := tx.State.UpdateOneID(row.ID).
							SetNotFoundStreak(newStreak).
							SetRetryCount(newRetryCount).
							SetUpdatedAt(time.Now().UTC()).
							Save(ctx)
						return e
					}); dbErr != nil {
						ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
						ilogger.T(ctx).WithError(dbErr).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Error("ticks/T2: streak update FAILED")
						return
					}
					ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false}).Trace("after query")
					row.NotFoundStreak = newStreak
					ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("before call: executeValidation")
					executeValidation(ctx, d, row, preclaimPrev)
					ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("after call: executeValidation")
				} else {
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "streak": newStreak}).Trace("branch: T2_streak_below_threshold")
					ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "streak": newStreak}).Info("ticks/T2: NOT_FOUND → NOT_FOUND (below threshold)")
					ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save"}).Trace("before query")
					if dbErr := d.Execute(ctx, func(tx *ent.Tx) error {
						_, e := tx.State.UpdateOneID(row.ID).
							SetNotFoundStreak(newStreak).
							SetRetryCount(newRetryCount).
							SetPreviousStatus(state.PreviousStatusPROCESSED).
							SetStatus(state.StatusNOT_FOUND).
							SetUpdatedAt(time.Now().UTC()).
							Save(ctx)
						return e
					}); dbErr != nil {
						ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
						ilogger.T(ctx).WithError(dbErr).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Error("ticks/T2: NOT_FOUND update FAILED")
						return
					}
					ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false}).Trace("after query")
				}
			} else {
				// COMPLETED + unexpected 404: data was there at T-0 time — mark as anomaly.
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Trace("branch: T2_completed+404_broken")
				ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Warn("ticks/T2: COMPLETED got 404 → BROKEN (data missing anomaly)")
				ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("before call: updateStateBroken")
				updateStateBroken(ctx, d, row)
				ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("after call: updateStateBroken")
			}
		} else {
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Trace("branch: T2_download_error")
			ilogger.T(ctx).WithError(dl.Err).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Error("ticks/T2: download ERROR → FAILED")
			ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("before call: updateStateFailed")
			updateStateFailed(ctx, d, row)
			ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("after call: updateStateFailed")
		}
		return
	}

	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "bytes": len(dl.Data)}).Trace("branch: T2_download_ok")
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "bytes": len(dl.Data)}).Info("ticks/T2: download OK")
	if wasNotFound {
		// NOT_FOUND + data now available: reset streak, hand off to next cycle via PENDING.
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Trace("branch: T2_not_found_recovered")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Info("ticks/T2: NOT_FOUND data recovered → streak=0 PENDING")
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save"}).Trace("before query")
		if err := d.Execute(ctx, func(tx *ent.Tx) error {
			_, e := tx.State.UpdateOneID(row.ID).
				SetNotFoundStreak(0).
				SetPreviousStatus(state.PreviousStatusNOT_FOUND).
				SetStatus(state.StatusPENDING).
				SetUpdatedAt(time.Now().UTC()).
				Save(ctx)
			return e
		}); err != nil {
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
			ilogger.T(ctx).WithError(err).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Error("ticks/T2: NOT_FOUND→PENDING FAILED")
			return
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false}).Trace("after query")
		return
	}

	// COMPLETED + 2xx: full convert + upload (overwrite) + validate → CONFIRMED.
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Trace("branch: T2_completed_revalidate")
	if row.NotFoundStreak > 0 {
		ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("before call: resetNotFoundStreak")
		resetNotFoundStreak(ctx, d, row, "T2 reset streak")
		ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("after call: resetNotFoundStreak")
	}
	var uploadErr error
	func() {
		ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("before tickProcessSema acquire")
		tickProcessSem <- struct{}{}
		ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("after tickProcessSema acquire")
		defer func() { <-tickProcessSem }()
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeT2Action", "instrument": inst}).Trace("before call: convertBI5ToParquet")
		parquet, convErr := convertBI5ToParquet(ctx, dl.Data, row)
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeT2Action", "err": convErr != nil}).Trace("after call: convertBI5ToParquet")
		if convErr != nil {
			ilogger.T(ctx).WithError(convErr).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Error("ticks/T2: convert FAILED")
			uploadErr = convErr
			return
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "bytes": len(parquet)}).Info("ticks/T2: convert OK")
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeT2Action", "instrument": inst}).Trace("before r2 upload")
		upErr := uploadToR2(ctx, row, parquet)
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeT2Action", "err": upErr != nil}).Trace("after r2 upload")
		if upErr != nil {
			ilogger.T(ctx).WithError(upErr).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Error("ticks/T2: upload FAILED")
			uploadErr = upErr
			return
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Info("ticks/T2: upload OK → validating")
	}()
	if uploadErr != nil {
		ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("before call: updateStateFailed")
		updateStateFailed(ctx, d, row)
		ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("after call: updateStateFailed")
		return
	}
	ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("before call: executeValidation")
	executeValidation(ctx, d, row, preclaimPrev)
	ilogger.T(ctx).WithField("fn", "executeT2Action").Trace("after call: executeValidation")
}

// executeValidation reads the Parquet file from R2 and validates it physically.
// On success the row is promoted to CONFIRMED with a SyncTask event.
// On failure the row is set to BROKEN; a SyncTask is inserted only when
// preclaimPrev == CONFIRMED, which indicates a genuine demotion: the row had
// already been confirmed in a previous cycle and is now found to be BROKEN.
func executeValidation(ctx context.Context, d *dal.DAL, row *ent.State, preclaimPrev *state.PreviousStatus) {
	inst := instrName(row)
	ts := row.Timestamp.Format(time.RFC3339)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeValidation", "instrument": inst, "ts": ts}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "executeValidation").Trace("fn_exit")

	start := time.Now()
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Info("ticks/validate: reading R2")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeValidation", "instrument": inst}).Trace("before r2 read")
	fileBytes, err := readFromR2(ctx, row)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeValidation", "err": err != nil}).Trace("after r2 read")
	if err != nil {
		ilogger.T(ctx).WithError(err).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "elapsed": time.Since(start).Round(time.Millisecond)}).Error("ticks/validate: R2 read FAILED → BROKEN")
		// Unable to read the file — treat as BROKEN so Backfill can retry.
		ilogger.T(ctx).WithField("fn", "executeValidation").Trace("before call: updateStateBroken")
		updateStateBroken(ctx, d, row)
		ilogger.T(ctx).WithField("fn", "executeValidation").Trace("after call: updateStateBroken")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "bytes": len(fileBytes), "elapsed": time.Since(start).Round(time.Millisecond)}).Info("ticks/validate: validating")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeValidation", "instrument": inst, "bytes": len(fileBytes)}).Trace("before call: validateParquetFile")
	validationErr := validateParquetFile(ctx, row, fileBytes)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeValidation", "err": validationErr != nil}).Trace("after call: validateParquetFile")
	if validationErr != nil {
		ilogger.T(ctx).WithError(validationErr).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Error("ticks/validate: validation FAILED → BROKEN")
	}

	ilogger.T(ctx).WithField("fn", "executeValidation").Trace("before call: updateValidatedTickStatus")
	if err := updateValidatedTickStatus(ctx, d, row, validationErr, preclaimPrev); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeValidation", "err": true}).Trace("after call: updateValidatedTickStatus")
		ilogger.T(ctx).WithError(err).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Error("ticks/validate: status update FAILED")
	} else {
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeValidation", "err": false}).Trace("after call: updateValidatedTickStatus")
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "elapsed": time.Since(start).Round(time.Millisecond)}).Info("ticks/validate: done")
}

// saveStateStatusInTx updates a state row's status inside an open transaction,
// emitting before/after trace logs. Returns the saved row so callers can read
// back fields like InstrumentID and Timestamp.
func saveStateStatusInTx(ctx context.Context, tx *ent.Tx, rowID uuid.UUID, prev state.PreviousStatus, next state.Status) (*ent.State, error) {
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "next": string(next)}).Trace("before query")
	s, e := tx.State.UpdateOneID(rowID).
		SetPreviousStatus(prev).
		SetStatus(next).
		SetUpdatedAt(time.Now().UTC()).
		Save(ctx)
	if e != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
		return nil, e
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false}).Trace("after query")
	return s, nil
}

func updateValidatedTickStatus(ctx context.Context, d *dal.DAL, row *ent.State, validationErr error, preclaimPrev *state.PreviousStatus) error {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "updateValidatedTickStatus", "instrument": instrName(row)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "updateValidatedTickStatus").Trace("fn_exit")

	return d.Execute(ctx, func(tx *ent.Tx) error {
		if validationErr == nil {
			saved, e := saveStateStatusInTx(ctx, tx, row.ID, state.PreviousStatusPROCESSED, state.StatusCONFIRMED)
			if e != nil {
				return e
			}
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "ts": saved.Timestamp.Format(time.RFC3339)}).Info("ticks/validate: CONFIRMED")
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "fn": "upsertSyncTaskInTx"}).Trace("before query")
			err := upsertSyncTaskInTx(ctx, tx, saved.InstrumentID, saved.Timestamp)
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "err": err != nil}).Trace("after query")
			return err
		}

		broken, e := saveStateStatusInTx(ctx, tx, row.ID, state.PreviousStatusPROCESSED, state.StatusBROKEN)
		if e != nil {
			return e
		}
		ilogger.T(ctx).WithFields(logrus.Fields{
			"status":          state.StatusBROKEN,
			"state_id":        broken.ID,
			"job_type":        broken.JobType,
			"previous_status": state.PreviousStatusPROCESSED,
			"message":         "tick parquet validation failed",
		}).Info("state_transition")
		if preclaimPrev != nil && *preclaimPrev == state.PreviousStatusCONFIRMED {
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "fn": "upsertSyncTaskInTx"}).Trace("before query")
			err := upsertSyncTaskInTx(ctx, tx, broken.InstrumentID, broken.Timestamp)
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "err": err != nil}).Trace("after query")
			return err
		}
		return nil
	})
}

// ── State transition helpers ──────────────────────────────────────────────────

func resetNotFoundStreak(ctx context.Context, d *dal.DAL, row *ent.State, logPrefix string) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "resetNotFoundStreak", "instrument": instrName(row)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "resetNotFoundStreak").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save"}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			SetNotFoundStreak(0).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).Errorf("%s for %s", logPrefix, row.ID)
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false}).Trace("after query")
}

// handleNotFoundSimple sets NotFoundStreak = 1 and status = NOT_FOUND.
// Used exclusively by T-0: a PENDING row in T-0 is always freshly seeded,
// so its streak is guaranteed to be 0. Setting to 1 is equivalent to +1
// and makes the first-attempt intent explicit.
// It never touches RetryCount and never triggers the Zero-Row Parquet flow.
func handleNotFoundSimple(ctx context.Context, d *dal.DAL, row *ent.State) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "handleNotFoundSimple", "instrument": instrName(row)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "handleNotFoundSimple").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save"}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			SetNotFoundStreak(1).
			SetPreviousStatus(state.PreviousStatusPROCESSED).
			SetStatus(state.StatusNOT_FOUND).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).WithFields(logrus.Fields{"instrument": instrName(row), "ts": row.Timestamp.Format(time.RFC3339)}).Error("ticks/ETL: NOT_FOUND simple FAILED")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false}).Trace("after query")
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "ts": row.Timestamp.Format(time.RFC3339), "streak": 1}).Info("ticks/ETL: NOT_FOUND (first attempt)")
}

// handleNotFoundIncrement increments NotFoundStreak by 1 and sets status = NOT_FOUND.
// Used by Backfill Group 1: a PENDING row in backfill may have been reclaimed from
// a NOT_FOUND path (streak > 0), so Add is required instead of Set.
// Like the handleNotFoundSimple, it never touches RetryCount and never triggers Zero-Row.
func handleNotFoundIncrement(ctx context.Context, d *dal.DAL, row *ent.State) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "handleNotFoundIncrement", "instrument": instrName(row)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "handleNotFoundIncrement").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save"}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			AddNotFoundStreak(1).
			SetPreviousStatus(state.PreviousStatusPROCESSED).
			SetStatus(state.StatusNOT_FOUND).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).WithFields(logrus.Fields{"instrument": instrName(row), "ts": row.Timestamp.Format(time.RFC3339)}).Error("ticks/ETL: NOT_FOUND increment FAILED")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false}).Trace("after query")
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "ts": row.Timestamp.Format(time.RFC3339), "new_streak": row.NotFoundStreak + 1}).Info("ticks/ETL: NOT_FOUND (backfill increment)")
}

// handleRetryReset increments RetryCount atomically and either resets the row
// to PENDING (for another attempt) or transitions it to ABANDONED (≥ 5).
//
// Mutual Exclusive Mutator: whenever RetryCount is incremented, NotFoundStreak
// MUST be decremented in the same atomic transaction (floor 0), keeping the two
// error counters from accumulating independently across overlapping failure modes
// (e.g., a row that alternates between 404s and 503s).
// T-1 recovery never transitions to ABANDONED — retry is unbounded.
func handleRetryReset(ctx context.Context, d *dal.DAL, row *ent.State) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "handleRetryReset", "instrument": instrName(row), "retry_count": row.RetryCount}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "handleRetryReset").Trace("fn_exit")

	newStreak := row.NotFoundStreak - 1
	if newStreak < 0 {
		newStreak = 0
	}
	ilogger.T(ctx).WithFields(logrus.Fields{
		"old_streak": row.NotFoundStreak, "new_streak": newStreak,
		"retry_count_before": row.RetryCount, "retry_count_after": row.RetryCount + 1,
	}).Debug("ticks/T1: retry_reset vars")

	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save"}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			AddRetryCount(1).
			SetNotFoundStreak(newStreak).
			SetPreviousStatus(state.PreviousStatusFAILED).
			SetStatus(state.StatusPENDING).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).WithFields(logrus.Fields{"instrument": instrName(row), "ts": row.Timestamp.Format(time.RFC3339)}).Error("ticks/T1: retry_reset FAILED")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false}).Trace("after query")
	ilogger.T(ctx).WithFields(logrus.Fields{
		"instrument":  instrName(row),
		"ts":          row.Timestamp.Format(time.RFC3339),
		"retry_count": row.RetryCount + 1,
		"streak":      newStreak,
	}).Info("ticks/T1: retry_reset → PENDING")
}

func resetStateToPending(ctx context.Context, d *dal.DAL, row *ent.State) {
	updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusPENDING, "reset to PENDING")
}

func updateStateCompleted(ctx context.Context, d *dal.DAL, row *ent.State) {
	updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusCOMPLETED, "set COMPLETED")
}

func updateStateFailed(ctx context.Context, d *dal.DAL, row *ent.State) {
	updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "set FAILED")
}

func updateStateBroken(ctx context.Context, d *dal.DAL, row *ent.State) {
	updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusBROKEN, "set BROKEN")
}

// upsertSyncTaskInTx inserts a PENDING SyncTask for the specified instrument and day,
// or resets an existing one back to PENDING to trigger ResolvedTickCount recomputation.
// It must be called inside an Execute transaction.
func upsertSyncTaskInTx(ctx context.Context, tx *ent.Tx, instrumentID uuid.UUID, tickTimestamp time.Time) error {
	targetDate := tickTimestamp.Truncate(24 * time.Hour)

	return tx.ExecContext(ctx, `
		INSERT INTO ingestion.sync_tasks (id, instrument_id, target_date, status, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (instrument_id, target_date)
		DO UPDATE SET status = EXCLUDED.status
	`,
		uuid.New(),
		instrumentID,
		targetDate,
		string(synctask.StatusPENDING),
		time.Now().UTC(),
	)
}

// ── Shared sub-pipeline ───────────────────────────────────────────────────────

// executeConvertUpload acquires tickProcessSem, converts raw BI5 bytes to
// Parquet, uploads to R2, then updates the row to COMPLETED or FAILED.
// Extracted to avoid duplicating the same block in executeIngestionETL and
// executeNotFoundRecheck.
func executeConvertUpload(ctx context.Context, d *dal.DAL, row *ent.State, data []byte) {
	inst := instrName(row)
	ts := row.Timestamp.Format(time.RFC3339)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeConvertUpload", "instrument": inst, "ts": ts}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "executeConvertUpload").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "input_bytes": len(data)}).Debug("ticks/ETL: convert-upload input")
	ilogger.T(ctx).WithField("fn", "executeConvertUpload").Trace("before tickProcessSema acquire")
	tickProcessSem <- struct{}{}
	ilogger.T(ctx).WithField("fn", "executeConvertUpload").Trace("after tickProcessSema acquire")
	defer func() { <-tickProcessSem }()
	start := time.Now()

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeConvertUpload", "instrument": inst}).Trace("before call: convertBI5ToParquet")
	parquet, err := convertBI5ToParquet(ctx, data, row)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeConvertUpload", "err": err != nil}).Trace("after call: convertBI5ToParquet")
	if err != nil {
		ilogger.T(ctx).WithError(err).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Error("ticks/ETL: convert FAILED")
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "convert failed")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "bytes": len(parquet), "elapsed": time.Since(start).Round(time.Millisecond)}).Info("ticks/ETL: convert OK")

	if row.Edges.Instrument != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"key": r2.TickObjectKey(row.Edges.Instrument.Name, row.Timestamp), "bytes": len(parquet)}).Debug("ticks/ETL: uploading")
	}
	r2Key := ""
	if row.Edges.Instrument != nil {
		r2Key = r2.TickObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeConvertUpload", "key": r2Key}).Trace("before r2 upload")
	if err := uploadToR2(ctx, row, parquet); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeConvertUpload", "key": r2Key, "err": true}).Trace("after r2 upload")
		ilogger.T(ctx).WithError(err).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "elapsed": time.Since(start).Round(time.Millisecond)}).Error("ticks/ETL: upload FAILED")
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "upload failed")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "executeConvertUpload", "key": r2Key, "err": false}).Trace("after r2 upload")
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "elapsed": time.Since(start).Round(time.Millisecond)}).Info("ticks/ETL: upload OK → COMPLETED")
	ilogger.T(ctx).WithField("fn", "executeConvertUpload").Trace("before call: updateStateCompleted")
	updateStateCompleted(ctx, d, row)
	ilogger.T(ctx).WithField("fn", "executeConvertUpload").Trace("after call: updateStateCompleted")
}

// updateSimpleStatus writes a single status transition to the DB.
// The prev value is recorded in previous_status; next becomes the new status.
// Extracted to avoid duplicating the same Execute pattern across
// updateStateCompleted / updateStateFailed / updateStateBroken / resetStateToPending.
//
// NotFoundStreak is intentionally NOT touched here. The bidirectional counter-rule
// (non-404 failure → streak--, floor 0) is applied exclusively inside handleRetryReset,
// which reads fresh DB values at claim time. Applying it here would use stale in-memory
// row values and could corrupt the streak when resetNotFoundStreak already ran earlier
// in the same pipeline (e.g., executeIngestionETL resets streak to 0 on data-found,
// then executeConvertUpload fails — touching streak here would undo that reset).
func updateSimpleStatus(ctx context.Context, d *dal.DAL, row *ent.State, prev state.PreviousStatus, next state.Status, errMsg string) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "updateSimpleStatus", "prev": prev, "next": next}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "updateSimpleStatus").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "prev": prev, "next": next}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			SetPreviousStatus(prev).
			SetStatus(next).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).Errorf("%s for %s", errMsg, row.ID)
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false}).Trace("after query")
	ilogger.T(ctx).WithFields(logrus.Fields{
		"instrument":  instrName(row),
		"ts":          row.Timestamp.Format(time.RFC3339),
		"state_id":    row.ID,
		"retry_count": row.RetryCount,
		"streak":      row.NotFoundStreak,
		"prev":        prev,
		"next":        next,
	}).Debug("ticks: transition_detail")
	ilogger.T(ctx).WithFields(logrus.Fields{
		"status":          next,
		"state_id":        row.ID,
		"job_type":        row.JobType,
		"previous_status": prev,
		"message":         errMsg,
	}).Info("state_transition")
}

// ── External operations ───────────────────────────────────────────────────────
// All functions below require row.Edges.Instrument to be non-nil.
// The claim loops in this file use WithInstrument() to satisfy that contract.

// errNotFound is returned by downloadBI5 when Dukascopy responds with HTTP 404.
var errNotFound = errors.New("tick source: 404 not found")

func isNotFoundError(err error) bool { return errors.Is(err, errNotFound) }

// errRateLimited is returned by downloadBI5 when Dukascopy responds with HTTP 429.
var errRateLimited = errors.New("tick source: 429 rate limited")

func isRateLimitedError(err error) bool { return errors.Is(err, errRateLimited) }

// downloadBI5 downloads the LZMA-compressed BI5 tick file from Dukascopy for
// the instrument and UTC hour represented by row.  Returns errNotFound on 404.
//
// Must only be called from a download gate worker (runDownloadWorker) after a
// rate-gate token has been consumed. Direct calls bypass the concurrency budget.
func downloadBI5(ctx context.Context, row *ent.State) ([]byte, error) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "downloadBI5", "instrument": instrName(row)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "downloadBI5").Trace("fn_exit")

	if dukClient == nil {
		return nil, errClientsNotInitialized
	}
	if row.Edges.Instrument == nil {
		return nil, fmt.Errorf("downloadBI5: instrument edge not loaded for state %s", row.ID)
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "downloadBI5", "instrument": row.Edges.Instrument.Name}).Trace("before call: dukClient.FetchBI5")
	data, err := dukClient.FetchBI5(ctx, row.Edges.Instrument.Name, row.Timestamp)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "downloadBI5", "err": err != nil}).Trace("after call: dukClient.FetchBI5")
	if errors.Is(err, dukascopy.ErrNotFound) {
		return nil, errNotFound
	}
	if errors.Is(err, dukascopy.ErrTooManyRequests) {
		return nil, errRateLimited
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

// convertBI5ToParquet decompresses the raw BI5 bytes, converts each tick to the
// canonical Parquet schema (timestamp/instrument/bid/ask/bid_volume/ask_volume),
// and returns the in-memory Parquet file bytes.
//
// The instrument's Divider field is used to convert integer prices to actual
// decimal prices (e.g., divider=100,000 for EUR/USD, 1,000 for XAU/USD).
func convertBI5ToParquet(ctx context.Context, raw []byte, row *ent.State) ([]byte, error) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "convertBI5ToParquet", "instrument": instrName(row)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "convertBI5ToParquet").Trace("fn_exit")

	if row.Edges.Instrument == nil {
		return nil, fmt.Errorf("convertBI5ToParquet: instrument edge not loaded for state %s", row.ID)
	}
	inst := row.Edges.Instrument
	ilogger.T(ctx).WithFields(logrus.Fields{
		"instrument": inst.Name, "divider": inst.Divider, "raw_bytes": len(raw),
	}).Debug("ticks/ETL: convert input")
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "convertBI5ToParquet", "instrument": inst.Name}).Trace("before call: dukascopy.ParseBI5")
	ticks, err := dukascopy.ParseBI5(raw, row.Timestamp, inst.Divider)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "convertBI5ToParquet", "err": err != nil}).Trace("after call: dukascopy.ParseBI5")
	if err != nil {
		return nil, fmt.Errorf("convertBI5ToParquet: parse BI5: %w", err)
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "convertBI5ToParquet", "instrument": inst.Name, "ticks": len(ticks)}).Trace("parsed ticks count")
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "ts": row.Timestamp.Format(time.RFC3339), "ticks": len(ticks)}).Debug("ticks/ETL: parsed")
	if logrus.IsLevelEnabled(logrus.TraceLevel) && len(ticks) > 0 {
		first, last := ticks[0], ticks[len(ticks)-1]
		ilogger.T(ctx).WithFields(logrus.Fields{"ts": first.Timestamp.Format(time.RFC3339), "bid": first.Bid, "ask": first.Ask}).Trace("ticks/ETL: first_tick")
		ilogger.T(ctx).WithFields(logrus.Fields{"ts": last.Timestamp.Format(time.RFC3339), "bid": last.Bid, "ask": last.Ask}).Trace("ticks/ETL: last_tick")
	}

	rows := make([]tickparquet.Row, len(ticks))
	for i, t := range ticks {
		rows[i] = tickparquet.Row{
			Timestamp:  t.Timestamp.UnixMicro(),
			Instrument: inst.Name,
			Bid:        t.Bid,
			Ask:        t.Ask,
			BidVolume:  int64(t.BidVolume),
			AskVolume:  int64(t.AskVolume),
		}
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "convertBI5ToParquet", "instrument": inst.Name, "rows": len(rows)}).Trace("before call: tickparquet.Write")
	result, writeErr := tickparquet.Write(rows)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "convertBI5ToParquet", "err": writeErr != nil}).Trace("after call: tickparquet.Write")
	if writeErr == nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "parquet_bytes": len(result)}).Debug("ticks/ETL: convert output")
	}
	return result, writeErr
}

// uploadToR2 puts the Parquet bytes at the canonical R2 object path for this row.
//
// Path: ingestion/dukascopy/ticks/{instrument}/{YYYY}/{MM}/ticks-{instrument}-{YYYY-MM-DD}-{HH}.parquet
//
// This implements the Overwrite Policy (§6.2): calling on an existing key
// replaces the old file, which is the correct behavior on FAILED/BROKEN retries.
func uploadToR2(ctx context.Context, row *ent.State, data []byte) error {
	if r2Client == nil {
		return errClientsNotInitialized
	}
	if row.Edges.Instrument == nil {
		return fmt.Errorf("uploadToR2: instrument edge not loaded for state %s", row.ID)
	}
	key := r2.TickObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "uploadToR2", "key": key, "bytes": len(data)}).Trace("before r2 upload")
	err := r2Client.PutObject(ctx, key, data)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "uploadToR2", "key": key, "err": err != nil}).Trace("after r2 upload")
	return err
}

// readFromR2 downloads the Parquet file for this row from R2 so it can be
// validated. Returns r2.ErrNotFound if the object is absent.
func readFromR2(ctx context.Context, row *ent.State) ([]byte, error) {
	if r2Client == nil {
		return nil, errClientsNotInitialized
	}
	if row.Edges.Instrument == nil {
		return nil, fmt.Errorf("readFromR2: instrument edge not loaded for state %s", row.ID)
	}
	key := r2.TickObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "readFromR2", "key": key}).Trace("before r2 read")
	data, err := r2Client.GetObject(ctx, key)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "readFromR2", "key": key, "err": err != nil}).Trace("after r2 read")
	return data, err
}

// validateParquetFile runs the four-step physical validation from §6.3:
//  1. The file size is greater than zero, and PAR1 magic bytes are present.
//  2. The Parquet footer can be parsed by parquet-go.
//  3. The schema has six columns in the expected order.
//  4. Each timestamp falls within the expected 1-hour window.
//
// Zero-row files are accepted only if the row is marked as a holiday (step 4 skipped).
func validateParquetFile(_ context.Context, row *ent.State, data []byte) error {
	return tickparquet.Validate(data, row.IsHoliday, row.Timestamp.UTC())
}

// buildZeroRowParquet returns a valid Parquet file with the Tick schema but
// zero data rows. Used when committing a confirmed market-holiday hour.
func buildZeroRowParquet() []byte {
	data, err := tickparquet.WriteZeroRow()
	if err != nil {
		// WriteZeroRow only fails if the in-memory buffer errors, which should
		// never happen in practice.
		return nil
	}
	return data
}

// recoverGoroutine catches any panic and logs a fallback message.
// It must be deferred AFTER wg.Done().
func recoverGoroutine(ctx context.Context, name string) {
	if r := recover(); r != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{
			"goroutine": name,
			"panic":     r,
			"stack":     string(debug.Stack()),
		}).Error("recovered panic")
	}
}
