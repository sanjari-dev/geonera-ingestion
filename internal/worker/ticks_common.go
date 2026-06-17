package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"entgo.io/ent/dialect/sql"
	"github.com/google/uuid"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/instrument"
	"github.com/sanjari-dev/geonera-ingestion/ent/predicate"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/ent/synctask"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/dukascopy"
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
// to the UUID string when the edge was not eagerly loaded (e.g. queries missing
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

	log.Printf("worker: download rate limiter started — max %d req/s, burst %d, interval %s",
		dukascopyMaxRPS, dukascopyBurst, interval)
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
	if onStarted != nil {
		onStarted()
	}
	log.Printf("ticks: handler start mode=%s", mode)

	runCtx := ctx
	var cancel context.CancelFunc
	switch mode {
	case "REGULAR":
		runCtx, cancel = context.WithTimeout(ctx, regularTimeout)
		defer cancel()
	case "BACKFILL":
		runCtx, cancel = context.WithTimeout(ctx, backfillTimeout)
		defer cancel()
	}

	var wg sync.WaitGroup
	switch mode {
	case "REGULAR":
		wg.Add(3)
		go runT0Phase(runCtx, d, &wg)
		go runT1Phase(runCtx, d, &wg)
		go runT2Phase(runCtx, d, &wg)
	case "BACKFILL":
		wg.Add(1)
		go runBackfillMasterLoop(runCtx, d, &wg)
	default:
		log.Printf("ticks: unknown mode %q", mode)
	}
	wg.Wait()

	if runCtx.Err() == context.DeadlineExceeded {
		switch mode {
		case "REGULAR":
			log.Printf("ticks/REGULAR: deadline exceeded (%s) — cycle was abnormal, all phases canceled", regularTimeout)
		case "BACKFILL":
			log.Printf("ticks/BACKFILL: deadline exceeded (%s) — batch goroutines canceled, goroutine leak prevented", backfillTimeout)
		}
	}

	log.Printf("ticks: handler done mode=%s", mode)
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
	start := time.Now()
	log.Printf("ticks/ETL: start instrument=%s ts=%s", inst, ts)

	// Phase 1: Download BI5 — submitted to the download gate worker pool.
	// The gate manages concurrency (dukascopyBurst workers) and rate limiting
	// (dukascopyMaxRPS tokens/s). The call respects ctx cancellation at every
	// blocking point: enqueue, rate-gate wait, and result wait.
	dl := RequestDownload(ctx, row, highPriority)
	switch dl.Status {
	case DownloadNotFound:
		log.Printf("ticks/ETL: download NOT_FOUND instrument=%s ts=%s elapsed=%s", inst, ts, time.Since(start).Round(time.Millisecond))
		onNotFound(ctx, d, row)
		return
	case DownloadOK:
		log.Printf("ticks/ETL: download OK instrument=%s ts=%s bytes=%d elapsed=%s", inst, ts, len(dl.Data), time.Since(start).Round(time.Millisecond))
	default: // DownloadRateLimited, DownloadError, or ctx canceled
		log.Printf("ticks/ETL: download ERROR instrument=%s ts=%s err=%v elapsed=%s → FAILED", inst, ts, dl.Err, time.Since(start).Round(time.Millisecond))
		updateStateFailed(ctx, d, row)
		return
	}

	// Data was found: reset NotFoundStreak to 0 if it was previously non-zero.
	if row.NotFoundStreak > 0 {
		resetNotFoundStreak(ctx, d, row, "reset streak")
	}

	// Phase 2: Convert + Upload — semaphore capped at runtime.NumCPU().
	executeConvertUpload(ctx, d, row, dl.Data)
	log.Printf("ticks/ETL: pipeline done instrument=%s ts=%s total_elapsed=%s", inst, ts, time.Since(start).Round(time.Millisecond))
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
	log.Printf("ticks/recheck: start instrument=%s ts=%s streak=%d", inst, ts, row.NotFoundStreak)

	dl := RequestDownload(ctx, row, highPriority)
	if dl.Status != DownloadOK {
		if dl.Status == DownloadNotFound {
			// Still 404: increment streak, mark holiday, upload Zero-Row Parquet.
			// DB commit happens before R2 upload, so IsHoliday flag persists even on
			// upload failure. If the upload fails, the row is set to FAILED so that
			// T-1/Backfill can retry the full pipeline on the next cycle.
			newStreak := row.NotFoundStreak + 1
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
				log.Printf("ticks/recheck: DB update FAILED instrument=%s ts=%s err=%v", inst, ts, dbErr)
				return
			}
			log.Printf("ticks/recheck: still NOT_FOUND instrument=%s ts=%s new_streak=%d → uploading zero-row", inst, ts, newStreak)
			zeroRow := buildZeroRowParquet()
			if upErr := uploadToR2(ctx, row, zeroRow); upErr != nil {
				log.Printf("ticks/recheck: zero-row upload FAILED instrument=%s ts=%s err=%v → FAILED", inst, ts, upErr)
				updateStateFailed(ctx, d, row)
			} else {
				log.Printf("ticks/recheck: zero-row uploaded instrument=%s ts=%s → NOT_FOUND", inst, ts)
			}
		} else {
			log.Printf("ticks/recheck: download ERROR instrument=%s ts=%s err=%v → FAILED", inst, ts, dl.Err)
			updateStateFailed(ctx, d, row)
		}
		return
	}

	// Data is available again: reset streak to 0, set PENDING.
	// The row will be picked up for a full ETL pass on the next cycle.
	log.Printf("ticks/recheck: data found instrument=%s ts=%s → streak=0 PENDING", inst, ts)
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			SetNotFoundStreak(0).
			SetPreviousStatus(state.PreviousStatusNOT_FOUND).
			SetStatus(state.StatusPENDING).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		log.Printf("ticks/recheck: reset to PENDING FAILED instrument=%s ts=%s err=%v", inst, ts, err)
	}
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
	log.Printf("ticks/T2: action start instrument=%s ts=%s prev_status=%s streak=%d",
		inst, ts, prevStatusStr(row.PreviousStatus), row.NotFoundStreak)

	dl := RequestDownload(ctx, row, highPriority)
	wasNotFound := row.PreviousStatus != nil && *row.PreviousStatus == state.PreviousStatusNOT_FOUND

	if dl.Status != DownloadOK {
		if dl.Status == DownloadNotFound {
			if wasNotFound {
				// NOT_FOUND + 404: increment streak.
				// At ≥ notFoundThreshold: validate the Zero-Row Parquet that T-1 already
				// uploaded to R2 → CONFIRMED (same validation path as COMPLETED+2xx).
				// Below threshold: set NOT_FOUND and wait for the next cycle.
				newStreak := row.NotFoundStreak + 1
				newRetryCount := row.RetryCount - 1
				if newRetryCount < 0 {
					newRetryCount = 0
				}
				if newStreak >= notFoundThreshold {
					log.Printf("ticks/T2: NOT_FOUND streak=%d >= threshold=%d instrument=%s ts=%s → validate zero-row",
						newStreak, notFoundThreshold, inst, ts)
					if dbErr := d.Execute(ctx, func(tx *ent.Tx) error {
						_, e := tx.State.UpdateOneID(row.ID).
							SetNotFoundStreak(newStreak).
							SetRetryCount(newRetryCount).
							SetUpdatedAt(time.Now().UTC()).
							Save(ctx)
						return e
					}); dbErr != nil {
						log.Printf("ticks/T2: streak update FAILED instrument=%s ts=%s err=%v", inst, ts, dbErr)
						return
					}
					row.NotFoundStreak = newStreak
					executeValidation(ctx, d, row, preclaimPrev)
				} else {
					log.Printf("ticks/T2: NOT_FOUND streak=%d instrument=%s ts=%s → NOT_FOUND (below threshold)", newStreak, inst, ts)
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
						log.Printf("ticks/T2: NOT_FOUND update FAILED instrument=%s ts=%s err=%v", inst, ts, dbErr)
					}
				}
			} else {
				// COMPLETED + unexpected 404: data was there at T-0 time — mark as anomaly.
				log.Printf("ticks/T2: COMPLETED got 404 instrument=%s ts=%s → BROKEN (data missing anomaly)", inst, ts)
				updateStateBroken(ctx, d, row)
			}
		} else {
			log.Printf("ticks/T2: download ERROR instrument=%s ts=%s err=%v → FAILED", inst, ts, dl.Err)
			updateStateFailed(ctx, d, row)
		}
		return
	}

	log.Printf("ticks/T2: download OK instrument=%s ts=%s bytes=%d", inst, ts, len(dl.Data))
	if wasNotFound {
		// NOT_FOUND + data now available: reset streak, hand off to next cycle via PENDING.
		log.Printf("ticks/T2: NOT_FOUND data recovered instrument=%s ts=%s → streak=0 PENDING", inst, ts)
		if err := d.Execute(ctx, func(tx *ent.Tx) error {
			_, e := tx.State.UpdateOneID(row.ID).
				SetNotFoundStreak(0).
				SetPreviousStatus(state.PreviousStatusNOT_FOUND).
				SetStatus(state.StatusPENDING).
				SetUpdatedAt(time.Now().UTC()).
				Save(ctx)
			return e
		}); err != nil {
			log.Printf("ticks/T2: NOT_FOUND→PENDING FAILED instrument=%s ts=%s err=%v", inst, ts, err)
		}
		return
	}

	// COMPLETED + 2xx: full convert + upload (overwrite) + validate → CONFIRMED.
	if row.NotFoundStreak > 0 {
		resetNotFoundStreak(ctx, d, row, "T2 reset streak")
	}
	var uploadErr error
	func() {
		tickProcessSem <- struct{}{}
		defer func() { <-tickProcessSem }()
		parquet, convErr := convertBI5ToParquet(dl.Data, row)
		if convErr != nil {
			log.Printf("ticks/T2: convert FAILED instrument=%s ts=%s err=%v", inst, ts, convErr)
			uploadErr = convErr
			return
		}
		log.Printf("ticks/T2: convert OK instrument=%s ts=%s parquet_bytes=%d", inst, ts, len(parquet))
		if upErr := uploadToR2(ctx, row, parquet); upErr != nil {
			log.Printf("ticks/T2: upload FAILED instrument=%s ts=%s err=%v", inst, ts, upErr)
			uploadErr = upErr
			return
		}
		log.Printf("ticks/T2: upload OK instrument=%s ts=%s → validating", inst, ts)
	}()
	if uploadErr != nil {
		updateStateFailed(ctx, d, row)
		return
	}
	executeValidation(ctx, d, row, preclaimPrev)
}

// executeValidation reads the Parquet file from R2 and validates it physically.
// On success the row is promoted to CONFIRMED with a SyncTask event.
// On failure the row is set to BROKEN; a SyncTask is inserted only when
// preclaimPrev == CONFIRMED, which indicates a genuine demotion: the row had
// already been confirmed in a previous cycle and is now found to be BROKEN.
func executeValidation(ctx context.Context, d *dal.DAL, row *ent.State, preclaimPrev *state.PreviousStatus) {
	inst := instrName(row)
	ts := row.Timestamp.Format(time.RFC3339)
	start := time.Now()
	log.Printf("ticks/validate: reading R2 instrument=%s ts=%s", inst, ts)

	fileBytes, err := readFromR2(ctx, row)
	if err != nil {
		log.Printf("ticks/validate: R2 read FAILED instrument=%s ts=%s err=%v elapsed=%s → BROKEN", inst, ts, err, time.Since(start).Round(time.Millisecond))
		// Unable to read the file — treat as BROKEN so Backfill can retry.
		updateStateBroken(ctx, d, row)
		return
	}
	log.Printf("ticks/validate: validating instrument=%s ts=%s bytes=%d read_elapsed=%s", inst, ts, len(fileBytes), time.Since(start).Round(time.Millisecond))

	validationErr := validateParquetFile(ctx, row, fileBytes)
	if validationErr != nil {
		log.Printf("ticks/validate: validation FAILED instrument=%s ts=%s err=%v → BROKEN", inst, ts, validationErr)
	}

	if err := updateValidatedTickStatus(ctx, d, row, validationErr, preclaimPrev); err != nil {
		log.Printf("ticks/validate: status update FAILED instrument=%s ts=%s err=%v", inst, ts, err)
	}
	log.Printf("ticks/validate: done instrument=%s ts=%s total_elapsed=%s", inst, ts, time.Since(start).Round(time.Millisecond))
}

func updateValidatedTickStatus(ctx context.Context, d *dal.DAL, row *ent.State, validationErr error, preclaimPrev *state.PreviousStatus) error {
	return d.Execute(ctx, func(tx *ent.Tx) error {
		if validationErr == nil {
			saved, e := tx.State.UpdateOneID(row.ID).
				SetPreviousStatus(state.PreviousStatusPROCESSED).
				SetStatus(state.StatusCONFIRMED).
				SetUpdatedAt(time.Now().UTC()).
				Save(ctx)
			if e != nil {
				return e
			}
				log.Printf("ticks/validate: CONFIRMED instrument=%s ts=%s",
				instrName(row), saved.Timestamp.Format(time.RFC3339))
			return upsertSyncTaskInTx(ctx, tx, saved.InstrumentID, saved.Timestamp)
		}

		broken, e := tx.State.UpdateOneID(row.ID).
			SetPreviousStatus(state.PreviousStatusPROCESSED).
			SetStatus(state.StatusBROKEN).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		if e != nil {
			return e
		}
		log.Printf(
			"state_transition status=%s state_id=%s job_type=%s previous_status=%s message=%q",
			state.StatusBROKEN,
			broken.ID,
			broken.JobType,
			state.PreviousStatusPROCESSED,
			"tick parquet validation failed",
		)
		if preclaimPrev != nil && *preclaimPrev == state.PreviousStatusCONFIRMED {
			return upsertSyncTaskInTx(ctx, tx, broken.InstrumentID, broken.Timestamp)
		}
		return nil
	})
}

// ── State transition helpers ──────────────────────────────────────────────────

func resetNotFoundStreak(ctx context.Context, d *dal.DAL, row *ent.State, logPrefix string) {
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			SetNotFoundStreak(0).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		log.Printf("%s for %s: %v", logPrefix, row.ID, err)
	}
}

// handleNotFoundSimple sets NotFoundStreak = 1 and status = NOT_FOUND.
// Used exclusively by T-0: a PENDING row in T-0 is always freshly seeded,
// so its streak is guaranteed to be 0. Setting to 1 is equivalent to +1
// and makes the first-attempt intent explicit.
// It never touches RetryCount and never triggers the Zero-Row Parquet flow.
func handleNotFoundSimple(ctx context.Context, d *dal.DAL, row *ent.State) {
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			SetNotFoundStreak(1).
			SetPreviousStatus(state.PreviousStatusPROCESSED).
			SetStatus(state.StatusNOT_FOUND).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		log.Printf("ticks/ETL: NOT_FOUND simple FAILED instrument=%s ts=%s err=%v",
			instrName(row), row.Timestamp.Format(time.RFC3339), err)
		return
	}
	log.Printf("ticks/ETL: NOT_FOUND instrument=%s ts=%s streak=1 (first attempt)",
		instrName(row), row.Timestamp.Format(time.RFC3339))
}

// handleNotFoundIncrement increments NotFoundStreak by 1 and sets status = NOT_FOUND.
// Used by Backfill Group 1: a PENDING row in backfill may have been reclaimed from
// a NOT_FOUND path (streak > 0), so Add is required instead of Set.
// Like the handleNotFoundSimple, it never touches RetryCount and never triggers Zero-Row.
func handleNotFoundIncrement(ctx context.Context, d *dal.DAL, row *ent.State) {
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			AddNotFoundStreak(1).
			SetPreviousStatus(state.PreviousStatusPROCESSED).
			SetStatus(state.StatusNOT_FOUND).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		log.Printf("ticks/ETL: NOT_FOUND increment FAILED instrument=%s ts=%s err=%v",
			instrName(row), row.Timestamp.Format(time.RFC3339), err)
		return
	}
	log.Printf("ticks/ETL: NOT_FOUND instrument=%s ts=%s new_streak=%d (backfill increment)",
		instrName(row), row.Timestamp.Format(time.RFC3339), row.NotFoundStreak+1)
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
	newStreak := row.NotFoundStreak - 1
	if newStreak < 0 {
		newStreak = 0
	}

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
		log.Printf("ticks/T1: retry_reset FAILED instrument=%s ts=%s err=%v",
			instrName(row), row.Timestamp.Format(time.RFC3339), err)
		return
	}
	log.Printf("ticks/T1: retry_reset instrument=%s ts=%s prev_retry=%d new_retry=%d new_streak=%d → PENDING",
		instrName(row), row.Timestamp.Format(time.RFC3339), row.RetryCount, row.RetryCount+1, newStreak)
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

	tickProcessSem <- struct{}{}
	defer func() { <-tickProcessSem }()
	start := time.Now()

	parquet, err := convertBI5ToParquet(data, row)
	if err != nil {
		log.Printf("ticks/ETL: convert FAILED instrument=%s ts=%s err=%v", inst, ts, err)
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "convert failed")
		return
	}
	log.Printf("ticks/ETL: convert OK instrument=%s ts=%s parquet_bytes=%d elapsed=%s", inst, ts, len(parquet), time.Since(start).Round(time.Millisecond))

	if err := uploadToR2(ctx, row, parquet); err != nil {
		log.Printf("ticks/ETL: upload FAILED instrument=%s ts=%s err=%v elapsed=%s", inst, ts, err, time.Since(start).Round(time.Millisecond))
		updateSimpleStatus(ctx, d, row, state.PreviousStatusPROCESSED, state.StatusFAILED, "upload failed")
		return
	}
	log.Printf("ticks/ETL: upload OK instrument=%s ts=%s elapsed=%s → COMPLETED", inst, ts, time.Since(start).Round(time.Millisecond))
	updateStateCompleted(ctx, d, row)
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
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.State.UpdateOneID(row.ID).
			SetPreviousStatus(prev).
			SetStatus(next).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		return e
	}); err != nil {
		log.Printf("%s for %s: %v", errMsg, row.ID, err)
		return
	}
	log.Printf(
		"state_transition status=%s state_id=%s job_type=%s previous_status=%s message=%q",
		next,
		row.ID,
		row.JobType,
		prev,
		errMsg,
	)
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
	if dukClient == nil {
		return nil, errClientsNotInitialized
	}
	if row.Edges.Instrument == nil {
		return nil, fmt.Errorf("downloadBI5: instrument edge not loaded for state %s", row.ID)
	}
	data, err := dukClient.FetchBI5(ctx, row.Edges.Instrument.Name, row.Timestamp)
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
func convertBI5ToParquet(raw []byte, row *ent.State) ([]byte, error) {
	if row.Edges.Instrument == nil {
		return nil, fmt.Errorf("convertBI5ToParquet: instrument edge not loaded for state %s", row.ID)
	}
	inst := row.Edges.Instrument

	ticks, err := dukascopy.ParseBI5(raw, row.Timestamp, inst.Divider)
	if err != nil {
		return nil, fmt.Errorf("convertBI5ToParquet: parse BI5: %w", err)
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
	return tickparquet.Write(rows)
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
	return r2Client.PutObject(ctx, key, data)
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
	return r2Client.GetObject(ctx, key)
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
		log.Printf("FATAL: panic in %s: %v\n%s", name, r, debug.Stack())
	}
}
