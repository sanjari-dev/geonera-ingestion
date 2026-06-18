package worker

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"entgo.io/ent/dialect/sql"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	ilogger "github.com/sanjari-dev/geonera-ingestion/internal/logger"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
)

func isR2NotFound(err error) bool { return errors.Is(err, r2.ErrNotFound) }

// maintenanceSeedBatchSize is the maximum number of rows inserted per DB
// transaction. 720 = 30 days × 24 h for TICK; for CANDLE the same limit
// covers ~2 years. Keeps individual transactions short to avoid lock contention
// and statement timeouts.
const maintenanceSeedBatchSize = 720

// consistencyBatchSize is the maximum number of missing timestamps fetched
// from PostgreSQL in a single QueryMissingTimestamps call. 1 000 000 rows ≈
// 114 years of TICK data — large enough to cover any single instrument in one
// or two calls under normal conditions, while bounding Go-side memory usage.
const consistencyBatchSize = 1_000_000

// maintenanceParallelLimit caps the number of instruments processed concurrently.
// Each concurrent instrument holds one dedicated advisory-lock connection plus up
// to 4 pool connections (one per Group-2 phase goroutine). At the limit of 8:
//
//	8 advisory + 8×4 pool = 40 connections reserved for maintenance.
//
// This keeps total usage well within PostgreSQL's default max_connections (100),
// even when other services (admin-backend, ch-sync) hold their own connections.
const maintenanceParallelLimit = 8

// deleteTickParquetFromR2 removes the hourly Tick Parquet file for this state
// row from Cloudflare R2.
func deleteTickParquetFromR2(ctx context.Context, row *ent.State) error {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "deleteTickParquetFromR2", "instrument": instrName(row)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "deleteTickParquetFromR2").Trace("fn_exit")

	if r2Client == nil {
		return errClientsNotInitialized
	}
	if row.Edges.Instrument == nil {
		return fmt.Errorf("deleteTickParquetFromR2: instrument edge not loaded for state %s", row.ID)
	}
	key := r2.TickObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "deleteTickParquetFromR2", "key": key}).Trace("before r2 delete")
	err := r2Client.DeleteObject(ctx, key)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "deleteTickParquetFromR2", "key": key, "err": err != nil}).Trace("after r2 delete")
	return err
}

// deleteCandleParquetFromR2 removes the daily Candle Parquet file for this
// state row from Cloudflare R2.
func deleteCandleParquetFromR2(ctx context.Context, row *ent.State) error {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "deleteCandleParquetFromR2", "instrument": instrName(row)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "deleteCandleParquetFromR2").Trace("fn_exit")

	if r2Client == nil {
		return errClientsNotInitialized
	}
	if row.Edges.Instrument == nil {
		return fmt.Errorf("deleteCandleParquetFromR2: instrument edge not loaded for state %s", row.ID)
	}
	key := r2.CandleObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "deleteCandleParquetFromR2", "key": key}).Trace("before r2 delete")
	err := r2Client.DeleteObject(ctx, key)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "deleteCandleParquetFromR2", "key": key, "err": err != nil}).Trace("after r2 delete")
	return err
}

// ── RunMaintenanceHandler ─────────────────────────────────────────────────────

// RunMaintenanceHandler iterates over every instrument (active and inactive).
// Per instrument, it either bootstraps a brand-new states table (Group 1) or
// runs the four normal-maintenance phases A/B/C/D in parallel (Group 2).
func RunMaintenanceHandler(ctx context.Context, d *dal.DAL, onStarted func()) bool {
	ilogger.T(ctx).WithField("fn", "RunMaintenanceHandler").Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "RunMaintenanceHandler").Trace("fn_exit")

	if onStarted != nil {
		onStarted()
	}
	logrus.Info("maintenance: running!")

	if actLogger != nil {
		ilogger.T(ctx).WithField("fn", "RunMaintenanceHandler").Trace("before call: actLogger.Purge")
		actLogger.Purge(ctx)
		ilogger.T(ctx).WithField("fn", "RunMaintenanceHandler").Trace("after call: actLogger.Purge")
	}

	var instruments []*ent.Instrument
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.Query.All"}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		var e error
		instruments, e = tx.Instrument.Query().All(ctx)
		return e
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.Query.All", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).Error("maintenance: query instruments")
		return true
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.Query.All", "count": len(instruments)}).Trace("after query")
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument_count": len(instruments), "parallel_limit": maintenanceParallelLimit}).Debug("maintenance: instrument vars")

	ilogger.T(ctx).WithFields(logrus.Fields{"count": len(instruments), "parallel_limit": maintenanceParallelLimit}).Info("maintenance: instruments to process")

	sem := make(chan struct{}, maintenanceParallelLimit)
	var wg sync.WaitGroup
	for _, inst := range instruments {
		if ctx.Err() != nil {
			ilogger.T(ctx).WithField("fn", "RunMaintenanceHandler").Trace("branch: ctx_canceled_break")
			break
		}
		inst := inst
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "RunMaintenanceHandler", "instrument": inst.Name}).Trace("before channel send: sem")
		sem <- struct{}{} // blocks when maintenanceParallelLimit goroutines are active
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "RunMaintenanceHandler", "instrument": inst.Name}).Trace("after channel send: sem")
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("goroutine: maintenance instrument start")
			runMaintenanceForInstrument(ctx, d, inst)
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("goroutine: maintenance instrument done")
		}()
	}
	ilogger.T(ctx).WithField("fn", "RunMaintenanceHandler").Trace("before wg.Wait")
	wg.Wait()
	ilogger.T(ctx).WithField("fn", "RunMaintenanceHandler").Trace("after wg.Wait")

	logrus.Info("maintenance: complete")
	return true
}

// instrumentLockKey derives a stable int64 advisory-lock key from an instrument
// UUID using FNV-1a. Collision probability is 1/2^64 — acceptable here because
// the worst outcome is a spurious skip of one maintenance cycle for one instrument.
func instrumentLockKey(id uuid.UUID) int64 {
	h := fnv.New64a()
	_, _ = h.Write(id[:])
	return int64(h.Sum64())
}

// runMaintenanceForInstrument acquires a per-instrument PostgreSQL advisory lock
// before dispatching Group 1 (bootstrap) or Group 2 (A + B + C + D in parallel).
// If a concurrent maintenance worker already holds the lock for this instrument,
// the call returns immediately without doing any work — the other worker is
// responsible for this instrument in the current cycle.
func runMaintenanceForInstrument(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runMaintenanceForInstrument", "instrument": inst.Name}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runMaintenanceForInstrument").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runMaintenanceForInstrument", "instrument": inst.Name}).Trace("before call: d.WithAdvisoryLock")
	locked, err := d.WithAdvisoryLock(ctx, instrumentLockKey(inst.ID), func() error {
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runMaintenanceForInstrument", "instrument": inst.Name}).Trace("advisory lock acquired")
		ilogger.T(ctx).WithFields(logrus.Fields{
			"instrument": inst.Name, "is_active": inst.IsActive, "is_pause": inst.IsPause,
			"start_date": inst.StartDate.UTC().Format(time.RFC3339),
		}).Debug("maintenance: instrument vars")
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runMaintenanceForInstrument", "instrument": inst.Name}).Trace("before call: isStatesEmpty")
		empty, err := isStatesEmpty(ctx, d, inst.ID)
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runMaintenanceForInstrument", "instrument": inst.Name, "empty": empty, "err": err != nil}).Trace("after call: isStatesEmpty")
		if err != nil {
			ilogger.T(ctx).WithError(err).Errorf("maintenance %s: check empty", inst.Name)
			return nil
		}

		if empty {
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("branch: maintenance_bootstrap")
			ilogger.T(ctx).WithField("instrument", inst.Name).Info("maintenance: starting Group 1 (bootstrap)")
			ilogger.T(ctx).WithField("instrument", inst.Name).Trace("before call: runBootstrap")
			runBootstrap(ctx, d, inst)
			ilogger.T(ctx).WithField("instrument", inst.Name).Trace("after call: runBootstrap")
			return nil
		}

		// Group 2: A + B + C + D run concurrently for this instrument.
		// IsPause is set lazily — the first goroutine that finds actual work
		// (insert or delete) calls setPause(), which uses sync.Once to ensure
		// the DB writing happens at most once. Goroutines with nothing to do
		// leave IsPause untouched for this cycle.
		// managePauseFlag is called after wg.Wait() so it sees the final
		// state of all four phases before deciding the correct IsPause value.
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("branch: maintenance_group2")
		ilogger.T(ctx).WithField("instrument", inst.Name).Info("maintenance: starting Group 2 (A+B+C+D)")

		var pauseOnce sync.Once
		setPause := func() {
			pauseOnce.Do(func() {
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.UpdateOneID.Save", "instrument": inst.Name}).Trace("before query")
				if err := d.Execute(ctx, func(tx *ent.Tx) error {
					_, e := tx.Instrument.UpdateOneID(inst.ID).SetIsPause(true).Save(ctx)
					return e
				}); err != nil {
					ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.UpdateOneID.Save", "err": true}).Trace("after query")
					ilogger.T(ctx).WithError(err).Errorf("maintenance %s Group 2: set IsPause=true", inst.Name)
					return
				}
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.UpdateOneID.Save", "err": false}).Trace("after query")
			})
		}

		var wg sync.WaitGroup
		wg.Add(4)
		go func() {
			defer wg.Done()
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("goroutine: forward-seeding start")
			runForwardSeeding(ctx, d, inst, setPause)
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("goroutine: forward-seeding done")
		}()
		go func() {
			defer wg.Done()
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("goroutine: backward-gap-fill start")
			runBackwardGapFill(ctx, d, inst, setPause)
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("goroutine: backward-gap-fill done")
		}()
		go func() {
			defer wg.Done()
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("goroutine: pruning start")
			runPruning(ctx, d, inst, setPause)
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("goroutine: pruning done")
		}()
		go func() {
			defer wg.Done()
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("goroutine: consistency-check start")
			runConsistencyCheck(ctx, d, inst, setPause)
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("goroutine: consistency-check done")
		}()

		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("before wg.Wait Group2")
		wg.Wait()
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("after wg.Wait Group2")

		ilogger.T(ctx).WithField("instrument", inst.Name).Trace("before call: managePauseFlag")
		managePauseFlag(ctx, d, inst)
		ilogger.T(ctx).WithField("instrument", inst.Name).Trace("after call: managePauseFlag")
		ilogger.T(ctx).WithField("instrument", inst.Name).Info("maintenance: Group 2 complete")
		return nil
	})
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runMaintenanceForInstrument", "instrument": inst.Name, "locked": locked, "err": err != nil}).Trace("after call: d.WithAdvisoryLock")
	if err != nil {
		ilogger.T(ctx).WithError(err).Errorf("maintenance %s: advisory lock", inst.Name)
		return
	}
	if !locked {
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("branch: advisory_lock_not_acquired")
		ilogger.T(ctx).WithField("instrument", inst.Name).Info("maintenance: skipped — lock held by concurrent worker")
	}
}

func isStatesEmpty(ctx context.Context, d *dal.DAL, instrumentID uuid.UUID) (bool, error) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "isStatesEmpty"}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "isStatesEmpty").Trace("fn_exit")

	var count int
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.Count"}).Trace("before query")
	err := d.Execute(ctx, func(tx *ent.Tx) error {
		var e error
		count, e = tx.State.Query().
			Where(
				state.InstrumentID(instrumentID),
				state.IsDeletedEQ(false),
			).
			Count(ctx)
		return e
	})
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.Count", "count": count, "err": err != nil}).Trace("after query")
	ilogger.T(ctx).WithFields(logrus.Fields{"count": count, "is_empty": count == 0}).Debug("maintenance: states count vars")
	return count == 0, err
}

// ── GROUP 1 — BOOTSTRAP ───────────────────────────────────────────────────────

// runBootstrap seeds the full PENDING row set for a brand-new (or fully-wiped)
// instrument. IsPause is set to true during seeding so that Tick/Candle workers
// skip this instrument while rows are being inserted. IsActive is never touched
// here — that flag is managed exclusively by engineers. All inserts use
// ON CONFLICT DO NOTHING, so the function is safe to retry after a partial run.
func runBootstrap(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	defer recoverGoroutine(ctx, "maintenance/bootstrap")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runBootstrap", "instrument": inst.Name}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runBootstrap").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.UpdateOneID.Save", "instrument": inst.Name}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.Instrument.UpdateOneID(inst.ID).SetIsPause(true).Save(ctx)
		return e
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.UpdateOneID.Save", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).Errorf("maintenance bootstrap %s: set IsPause=true", inst.Name)
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.UpdateOneID.Save", "err": false}).Trace("after query")

	// Log after IsPause is committed — if the DB writing above fails and is retried
	// on the next maintenance cycle, this line will not appear twice in the logs.
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "from": inst.StartDate.UTC().Format(time.RFC3339)}).Info("maintenance bootstrap: seeding")

	now := time.Now().UTC()
	for _, jobType := range []state.JobType{state.JobTypeTICK, state.JobTypeCANDLE} {
		if ctx.Err() != nil {
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("branch: bootstrap_ctx_canceled")
			break
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType}).Trace("loop: bootstrap job_type iteration start")
		interval := jobTypeInterval(jobType)
		startNorm := normalizeTS(inst.StartDate.UTC(), jobType)
		nowNorm := normalizeTS(now, jobType)

		var candidates []time.Time
		for ts := startNorm; !ts.After(nowNorm); ts = ts.Add(interval) {
			candidates = append(candidates, ts)
		}
		ilogger.T(ctx).WithFields(logrus.Fields{
			"instrument": inst.Name, "job_type": jobType,
			"start_norm": startNorm.Format(time.RFC3339), "now_norm": nowNorm.Format(time.RFC3339),
			"interval": interval, "slots": len(candidates),
		}).Debug("maintenance: bootstrap seed vars")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "slots": len(candidates), "job_type": jobType}).Info("maintenance bootstrap: seeding slots")
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runBootstrap", "instrument": inst.Name, "job_type": jobType, "slots": len(candidates)}).Trace("before call: insertBatched")
		insertBatched(ctx, d, inst.ID, jobType, candidates, fmt.Sprintf("bootstrap %s", inst.Name))
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runBootstrap", "instrument": inst.Name, "job_type": jobType}).Trace("after call: insertBatched")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType}).Trace("loop: bootstrap job_type iteration end")
	}

	ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.UpdateOneID.Save", "instrument": inst.Name}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.Instrument.UpdateOneID(inst.ID).SetIsPause(false).Save(ctx)
		return e
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.UpdateOneID.Save", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).Errorf("maintenance bootstrap %s: set IsPause=false", inst.Name)
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.UpdateOneID.Save", "err": false}).Trace("after query")
	ilogger.T(ctx).WithField("instrument", inst.Name).Info("maintenance bootstrap: complete")
}

// ── GROUP 2A — FORWARD SEEDING ────────────────────────────────────────────────

// runForwardSeeding inserts PENDING rows from MAX(timestamp)+interval up to
// time.Now() for both TICK and CANDLE, in batches. No upper-window cap —
// the full range is always covered in one maintenance cycle.
func runForwardSeeding(ctx context.Context, d *dal.DAL, inst *ent.Instrument, setPause func()) {
	defer recoverGoroutine(ctx, "maintenance/forward-seeding")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runForwardSeeding", "instrument": inst.Name}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runForwardSeeding").Trace("fn_exit")

	for _, jobType := range []state.JobType{state.JobTypeTICK, state.JobTypeCANDLE} {
		if ctx.Err() != nil {
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("branch: forward_seeding_ctx_canceled")
			return
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType}).Trace("loop: forward seeding job_type iteration start")
		interval := jobTypeInterval(jobType)

		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runForwardSeeding", "instrument": inst.Name, "job_type": jobType}).Trace("before call: queryStateMaxTS")
		maxTS, err := queryStateMaxTS(ctx, d, inst.ID, jobType)
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runForwardSeeding", "err": err != nil}).Trace("after call: queryStateMaxTS")
		if err != nil {
			ilogger.T(ctx).WithError(err).Errorf("forward seeding %s %s: query max", inst.Name, jobType)
			continue
		}
		if maxTS.IsZero() {
			maxTS = normalizeTS(inst.StartDate.UTC(), jobType)
		}

		nowNorm := normalizeTS(time.Now().UTC(), jobType)
		ilogger.T(ctx).WithFields(logrus.Fields{
			"instrument": inst.Name, "job_type": jobType,
			"max_ts": maxTS.Format(time.RFC3339), "now_norm": nowNorm.Format(time.RFC3339),
		}).Debug("maintenance: forward seeding boundary vars")
		if !maxTS.Before(nowNorm) {
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType}).Trace("branch: forward_seeding_up_to_date")
			continue
		}

		var candidates []time.Time
		for ts := maxTS.Add(interval); !ts.After(nowNorm); ts = ts.Add(interval) {
			candidates = append(candidates, ts)
		}
		if len(candidates) == 0 {
			continue
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType, "candidates": len(candidates)}).Debug("maintenance: forward seeding candidate vars")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType, "rows": len(candidates)}).Trace("branch: forward_seeding_inserting")
		ilogger.T(ctx).WithField("fn", "runForwardSeeding").Trace("before call: setPause")
		setPause()
		ilogger.T(ctx).WithField("fn", "runForwardSeeding").Trace("after call: setPause")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType, "rows": len(candidates)}).Info("forward seeding: inserting new rows")
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runForwardSeeding", "instrument": inst.Name, "job_type": jobType, "rows": len(candidates)}).Trace("before call: insertBatched")
		insertBatched(ctx, d, inst.ID, jobType, candidates, fmt.Sprintf("forward-seeding %s %s", inst.Name, jobType))
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runForwardSeeding", "instrument": inst.Name, "job_type": jobType}).Trace("after call: insertBatched")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType}).Trace("loop: forward seeding job_type iteration end")
	}
}

// ── GROUP 2B — BACKWARD GAP FILL ─────────────────────────────────────────────

// runBackwardGapFill inserts PENDING rows from MIN(timestamp)-interval back to
// Instrument.StartDate for both TICK and CANDLE. Status is intentionally
// ignored — only the existence of a row at a given timestamp matters.
// After filling gaps, managePauseFlag checks for hard-stuck TICK rows and
// updates Instrument.IsPause accordingly.
func runBackwardGapFill(ctx context.Context, d *dal.DAL, inst *ent.Instrument, setPause func()) {
	defer recoverGoroutine(ctx, "maintenance/backward-gap-fill")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runBackwardGapFill", "instrument": inst.Name}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runBackwardGapFill").Trace("fn_exit")

	startDate := inst.StartDate.UTC()

	for _, jobType := range []state.JobType{state.JobTypeTICK, state.JobTypeCANDLE} {
		if ctx.Err() != nil {
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("branch: backward_gap_fill_ctx_canceled")
			return
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType}).Trace("loop: backward gap fill job_type iteration start")
		interval := jobTypeInterval(jobType)
		startNorm := normalizeTS(startDate, jobType)

		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runBackwardGapFill", "instrument": inst.Name, "job_type": jobType}).Trace("before call: queryStateMinTS")
		minTS, err := queryStateMinTS(ctx, d, inst.ID, jobType)
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runBackwardGapFill", "err": err != nil}).Trace("after call: queryStateMinTS")
		if err != nil {
			ilogger.T(ctx).WithError(err).Errorf("backward gap fill %s %s: query min", inst.Name, jobType)
			continue
		}

		ilogger.T(ctx).WithFields(logrus.Fields{
			"instrument": inst.Name, "job_type": jobType,
			"min_ts": minTS.Format(time.RFC3339), "start_norm": startNorm.Format(time.RFC3339),
		}).Debug("maintenance: backward gap fill boundary vars")
		if minTS.IsZero() || !minTS.After(startNorm) {
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType}).Trace("branch: backward_gap_fill_at_start")
			continue
		}

		var candidates []time.Time
		for ts := minTS.Add(-interval); !ts.Before(startNorm); ts = ts.Add(-interval) {
			candidates = append(candidates, ts)
		}
		if len(candidates) == 0 {
			continue
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType, "candidates": len(candidates)}).Debug("maintenance: backward gap fill candidate vars")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType, "rows": len(candidates)}).Trace("branch: backward_gap_fill_inserting")
		ilogger.T(ctx).WithField("fn", "runBackwardGapFill").Trace("before call: setPause")
		setPause()
		ilogger.T(ctx).WithField("fn", "runBackwardGapFill").Trace("after call: setPause")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType, "rows": len(candidates)}).Info("backward gap fill: filling rows toward StartDate")
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runBackwardGapFill", "instrument": inst.Name, "job_type": jobType, "rows": len(candidates)}).Trace("before call: insertBatched")
		insertBatched(ctx, d, inst.ID, jobType, candidates, fmt.Sprintf("backward-gap-fill %s %s", inst.Name, jobType))
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runBackwardGapFill", "instrument": inst.Name, "job_type": jobType}).Trace("after call: insertBatched")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType}).Trace("loop: backward gap fill job_type iteration end")
	}
}

// managePauseFlag sets Instrument.IsPause based on whether the TICK historical
// range has been fully seeded back to StartDate. It finds the earliest TICK row
// (any status) and compares it against Instrument.StartDate:
//   - MIN > StartDate → gaps remain → IsPause = true
//   - MIN <= StartDate → range fully covered → IsPause = false
//
// Status is intentionally ignored — the backward gap fills already inserted
// PENDING rows for every missing slot; whether those rows haven't been processed
// yet is irrelevant to the pause decision.
func managePauseFlag(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "managePauseFlag", "instrument": inst.Name}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "managePauseFlag").Trace("fn_exit")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "managePauseFlag", "instrument": inst.Name}).Trace("before call: queryStateMinTS")
	minTS, err := queryStateMinTS(ctx, d, inst.ID, state.JobTypeTICK)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "managePauseFlag", "err": err != nil}).Trace("after call: queryStateMinTS")
	if err != nil {
		ilogger.T(ctx).WithError(err).Errorf("manage pause %s: query min tick", inst.Name)
		return
	}
	if minTS.IsZero() {
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("branch: manage_pause_min_zero")
		return
	}

	startNorm := normalizeTS(inst.StartDate.UTC(), state.JobTypeTICK)
	shouldPause := minTS.After(startNorm)
	ilogger.T(ctx).WithFields(logrus.Fields{
		"instrument": inst.Name, "min_ts": minTS.Format(time.RFC3339),
		"start_norm": startNorm.Format(time.RFC3339), "should_pause": shouldPause,
	}).Debug("maintenance: manage pause vars")
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "should_pause": shouldPause}).Trace("branch: manage_pause_decision")

	ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.UpdateOneID.Save", "instrument": inst.Name, "should_pause": shouldPause}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		_, e := tx.Instrument.UpdateOneID(inst.ID).SetIsPause(shouldPause).Save(ctx)
		return e
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.UpdateOneID.Save", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).Errorf("manage pause %s: set IsPause=%v", inst.Name, shouldPause)
	} else {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "Instrument.UpdateOneID.Save", "err": false}).Trace("after query")
		ilogger.T(ctx).WithFields(logrus.Fields{
			"instrument": inst.Name,
			"is_pause":   shouldPause,
			"min_ts":     minTS.Format(time.RFC3339),
			"start_date": normalizeTS(inst.StartDate.UTC(), state.JobTypeTICK).Format(time.RFC3339),
		}).Info("manage pause: IsPause updated")
	}
}

// ── GROUP 2C — PRUNING ────────────────────────────────────────────────────────

// runPruning soft-deletes rows below StartDate (Phase 1), sweeps the
// corresponding R2 objects (Phase 2), then hard-deletes the succeeded group
// from the DB (Phase 3). Both phases are per-instrument and batched.
func runPruning(ctx context.Context, d *dal.DAL, inst *ent.Instrument, setPause func()) {
	defer recoverGoroutine(ctx, "maintenance/pruning")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runPruning", "instrument": inst.Name}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runPruning").Trace("fn_exit")

	var hasWork bool
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.Exist", "instrument": inst.Name}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		var e error
		hasWork, e = tx.State.Query().
			Where(
				state.InstrumentID(inst.ID),
				state.Or(
					state.And(state.TimestampLT(inst.StartDate), state.IsDeletedEQ(false)),
					state.IsDeletedEQ(true),
				),
			).
			Exist(ctx)
		return e
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.Exist", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).Errorf("pruning %s: check", inst.Name)
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.Exist", "has_work": hasWork}).Trace("after query")
	ilogger.T(ctx).WithFields(logrus.Fields{
		"instrument": inst.Name, "start_date": inst.StartDate.UTC().Format(time.RFC3339), "has_work": hasWork,
	}).Debug("maintenance: pruning vars")
	if !hasWork {
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("branch: pruning_no_work")
		return
	}
	ilogger.T(ctx).WithField("fn", "runPruning").Trace("before call: setPause")
	setPause()
	ilogger.T(ctx).WithField("fn", "runPruning").Trace("after call: setPause")
	ilogger.T(ctx).WithField("fn", "runPruning").Trace("before call: runPruningPhase1Mark")
	runPruningPhase1Mark(ctx, d, inst)
	ilogger.T(ctx).WithField("fn", "runPruning").Trace("after call: runPruningPhase1Mark")
	ilogger.T(ctx).WithField("fn", "runPruning").Trace("before call: runPruningPhase2Sweep")
	runPruningPhase2Sweep(ctx, d, inst)
	ilogger.T(ctx).WithField("fn", "runPruning").Trace("after call: runPruningPhase2Sweep")
}

// runPruningPhase1Mark soft-deletes rows WHERE timestamp < StartDate AND
// is_deleted=false for this instrument, in batches of 100.
func runPruningPhase1Mark(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runPruningPhase1Mark", "instrument": inst.Name}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runPruningPhase1Mark").Trace("fn_exit")

	totalMarked := 0
	for ctx.Err() == nil {
		ilogger.T(ctx).WithField("fn", "runPruningPhase1Mark").Trace("loop: iteration start")
		var batchLen int
		if err := d.Execute(ctx, func(tx *ent.Tx) error {
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.All", "instrument": inst.Name}).Trace("before query")
			rows, e := tx.State.Query().
				Where(
					state.InstrumentID(inst.ID),
					state.TimestampLT(inst.StartDate),
					state.IsDeletedEQ(false),
				).
				Limit(100).
				All(ctx)
			if e != nil {
				ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.All", "err": true}).Trace("after query")
				return e
			}
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.All", "count": len(rows)}).Trace("after query")
			batchLen = len(rows)
			if batchLen == 0 {
				return nil
			}
			ids := make([]uuid.UUID, batchLen)
			for i, r := range rows {
				ids[i] = r.ID
			}
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Update.Exec", "count": batchLen}).Trace("before query")
			err := tx.State.Update().
				Where(state.IDIn(ids...)).
				SetIsDeleted(true).
				SetUpdatedAt(time.Now().UTC()).
				Exec(ctx)
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Update.Exec", "err": err != nil}).Trace("after query")
			return err
		}); err != nil {
			ilogger.T(ctx).WithError(err).Errorf("pruning phase1 mark %s", inst.Name)
			break
		}
		totalMarked += batchLen
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "batch_len": batchLen, "total_marked_so_far": totalMarked}).Debug("maintenance: pruning phase1 mark vars")
		if batchLen == 0 {
			break
		}
		ilogger.T(ctx).WithField("fn", "runPruningPhase1Mark").Trace("loop: iteration end")
	}
	if totalMarked > 0 {
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "rows": totalMarked}).Info("pruning phase1: marked rows as soft-deleted")
	}
}

// runPruningPhase2Sweep implements the two-phase R2 + DB cleanup:
//
// Phase 2 — R2 Sweep: load all is_deleted=true rows, attempt to delete each
// R2 object, and split results into succeeded (deleted or NoSuchKey) vs. failed
// (transient R2 error — left is_deleted=true for retry on the next cycle).
//
// Phase 3 — DB Hard Delete: bulk-delete only the succeeding group from the DB.
// Failed rows remain is_deleted=true and are retried automatically.
func runPruningPhase2Sweep(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runPruningPhase2Sweep", "instrument": inst.Name}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runPruningPhase2Sweep").Trace("fn_exit")

	// ── Phase 2: R2 Sweep ────────────────────────────────────────────────────
	// Load all is_deleted=true rows for this instrument. The set is bounded —
	// only rows below Instrument.StartDate are soft-deleted by Phase 1.
	var allRows []*ent.State
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.All", "instrument": inst.Name}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		var e error
		allRows, e = tx.State.Query().
			Where(
				state.InstrumentID(inst.ID),
				state.IsDeletedEQ(true),
			).
			WithInstrument().
			All(ctx)
		return e
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.All", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).Errorf("pruning phase2 %s: query IsDeleted rows", inst.Name)
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.All", "count": len(allRows)}).Trace("after query")
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "to_sweep": len(allRows)}).Debug("maintenance: pruning phase2 vars")
	if len(allRows) == 0 {
		return
	}

	var succeeded []uuid.UUID
	for _, row := range allRows {
		if ctx.Err() != nil {
			ilogger.T(ctx).WithField("fn", "runPruningPhase2Sweep").Trace("branch: r2_sweep_ctx_canceled")
			break
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": row.JobType, "state_id": row.ID, "ts": row.Timestamp.Format(time.RFC3339)}).Trace("loop: r2 sweep iteration start")
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": row.JobType, "state_id": row.ID}).Debug("maintenance: pruning sweep row vars")
		var r2Err error
		switch row.JobType {
		case state.JobTypeTICK:
			ilogger.T(ctx).WithField("fn", "runPruningPhase2Sweep").Trace("before call: deleteTickParquetFromR2")
			r2Err = deleteTickParquetFromR2(ctx, row)
			ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runPruningPhase2Sweep", "err": r2Err != nil}).Trace("after call: deleteTickParquetFromR2")
		case state.JobTypeCANDLE:
			ilogger.T(ctx).WithField("fn", "runPruningPhase2Sweep").Trace("before call: deleteCandleParquetFromR2")
			r2Err = deleteCandleParquetFromR2(ctx, row)
			ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runPruningPhase2Sweep", "err": r2Err != nil}).Trace("after call: deleteCandleParquetFromR2")
		}
		if r2Err != nil && !isR2NotFound(r2Err) {
			ilogger.T(ctx).WithError(r2Err).Errorf("pruning phase2 %s: R2 delete %s (%s) — will retry next cycle",
				inst.Name, row.ID, row.JobType)
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "state_id": row.ID, "err": true}).Trace("loop: r2 sweep iteration end (failed)")
		} else {
			succeeded = append(succeeded, row.ID)
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "state_id": row.ID, "succeeded_so_far": len(succeeded)}).Trace("loop: r2 sweep iteration end (ok)")
		}
	}

	failed := len(allRows) - len(succeeded)
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "total": len(allRows), "succeeded": len(succeeded), "failed": failed}).Debug("maintenance: pruning phase2 result vars")
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "total": len(allRows), "succeeded": len(succeeded), "failed": failed}).Info("pruning phase2: swept rows")

	// ── Phase 3: DB Hard Delete (succeeded group only) ───────────────────────
	if len(succeeded) == 0 {
		return
	}
	const pruneBatch = 500
	totalDeleted := 0
	for i := 0; i < len(succeeded); i += pruneBatch {
		ilogger.T(ctx).WithField("fn", "runPruningPhase2Sweep").Trace("loop: hard delete batch iteration")
		end := i + pruneBatch
		if end > len(succeeded) {
			end = len(succeeded)
		}
		chunk := succeeded[i:end]
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Delete.Exec", "count": len(chunk)}).Trace("before query")
		if err := d.Execute(ctx, func(tx *ent.Tx) error {
			_, e := tx.State.Delete().Where(state.IDIn(chunk...)).Exec(ctx)
			return e
		}); err != nil {
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Delete.Exec", "err": true}).Trace("after query")
			ilogger.T(ctx).WithError(err).Errorf("pruning phase3 %s: hard-delete", inst.Name)
		} else {
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Delete.Exec", "err": false, "count": len(chunk)}).Trace("after query")
			totalDeleted += len(chunk)
		}
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "rows": totalDeleted}).Info("pruning phase3: hard-deleted rows")
}

// ── GROUP 2D — CONSISTENCY CHECK ─────────────────────────────────────────────

// runConsistencyCheck verifies that every timestamp slot between MIN and MAX
// has a row in states. It runs independently for TICK (hourly) and CANDLE
// (daily). Missing slots are inserted as PENDING in batches.
func runConsistencyCheck(ctx context.Context, d *dal.DAL, inst *ent.Instrument, setPause func()) {
	defer recoverGoroutine(ctx, "maintenance/consistency-check")

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runConsistencyCheck", "instrument": inst.Name}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "runConsistencyCheck").Trace("fn_exit")

	for _, jobType := range []state.JobType{state.JobTypeTICK, state.JobTypeCANDLE} {
		if ctx.Err() != nil {
			ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name}).Trace("branch: consistency_check_ctx_canceled")
			return
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType}).Trace("loop: consistency check job_type iteration")
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runConsistencyCheck", "instrument": inst.Name, "job_type": jobType}).Trace("before call: checkConsistencyForJobType")
		checkConsistencyForJobType(ctx, d, inst, jobType, setPause)
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "runConsistencyCheck", "instrument": inst.Name, "job_type": jobType}).Trace("after call: checkConsistencyForJobType")
	}
}

func checkConsistencyForJobType(ctx context.Context, d *dal.DAL, inst *ent.Instrument, jobType state.JobType, setPause func()) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "checkConsistencyForJobType", "instrument": inst.Name, "job_type": jobType}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "checkConsistencyForJobType").Trace("fn_exit")

	interval := jobTypeInterval(jobType)

	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "checkConsistencyForJobType", "instrument": inst.Name, "job_type": jobType}).Trace("before call: queryStateMinTS")
	minTS, err := queryStateMinTS(ctx, d, inst.ID, jobType)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "checkConsistencyForJobType", "err": err != nil}).Trace("after call: queryStateMinTS")
	if err != nil {
		ilogger.T(ctx).WithError(err).Errorf("consistency check %s %s: query min", inst.Name, jobType)
		return
	}
	if minTS.IsZero() {
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType}).Trace("branch: consistency_check_no_rows")
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "checkConsistencyForJobType", "instrument": inst.Name, "job_type": jobType}).Trace("before call: queryStateMaxTS")
	maxTS, err := queryStateMaxTS(ctx, d, inst.ID, jobType)
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "checkConsistencyForJobType", "err": err != nil}).Trace("after call: queryStateMaxTS")
	if err != nil {
		ilogger.T(ctx).WithError(err).Errorf("consistency check %s %s: query max", inst.Name, jobType)
		return
	}

	expected := int(maxTS.Sub(minTS)/interval) + 1
	ilogger.T(ctx).WithFields(logrus.Fields{
		"instrument": inst.Name, "job_type": jobType,
		"min_ts": minTS.Format(time.RFC3339), "max_ts": maxTS.Format(time.RFC3339),
		"interval": interval, "expected": expected,
	}).Debug("maintenance: consistency range vars")

	var actual int
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.Count", "instrument": inst.Name, "job_type": jobType}).Trace("before query")
	if err := d.Execute(ctx, func(tx *ent.Tx) error {
		var e error
		actual, e = tx.State.Query().
			Where(
				state.InstrumentID(inst.ID),
				state.JobTypeEQ(jobType),
				state.TimestampGTE(minTS),
				state.TimestampLTE(maxTS),
				state.IsDeletedEQ(false),
			).
			Count(ctx)
		return e
	}); err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.Count", "err": true}).Trace("after query")
		ilogger.T(ctx).WithError(err).Errorf("consistency check %s %s: count actual", inst.Name, jobType)
		return
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.Count", "actual": actual, "expected": expected}).Trace("after query")
	ilogger.T(ctx).WithFields(logrus.Fields{
		"instrument": inst.Name, "job_type": jobType,
		"actual": actual, "expected": expected, "missing": expected - actual,
	}).Debug("maintenance: consistency count vars")

	if actual >= expected {
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst.Name, "job_type": jobType}).Trace("branch: consistency_check_ok")
		return
	}

	ilogger.T(ctx).WithField("fn", "checkConsistencyForJobType").Trace("before call: setPause")
	setPause()
	ilogger.T(ctx).WithField("fn", "checkConsistencyForJobType").Trace("after call: setPause")
	ilogger.T(ctx).WithFields(logrus.Fields{
		"instrument": inst.Name,
		"job_type":   jobType,
		"expected":   expected,
		"actual":     actual,
		"missing":    expected - actual,
		"batch_size": consistencyBatchSize,
	}).Info("consistency check: filling missing rows")

	// Cursor-based scan: afterTS starts one step before minTS so the first
	// batch includes minTS itself. Each iteration fetches up to consistencyBatchSize
	// missing timestamps, inserts them, then advances the cursor to the last
	// timestamp returned. Loop exits when the batch is smaller than the limit
	// (no more gaps) or ctx is canceled.
	cursor := minTS.Add(-interval)
	for ctx.Err() == nil {
		ilogger.T(ctx).WithField("fn", "checkConsistencyForJobType").Trace("loop: consistency cursor iteration start")

		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "checkConsistencyForJobType", "instrument": inst.Name}).Trace("before call: d.QueryMissingTimestamps")
		missing, err := d.QueryMissingTimestamps(ctx, inst.ID, string(jobType), minTS, maxTS, interval, cursor, consistencyBatchSize)
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "checkConsistencyForJobType", "missing": len(missing), "err": err != nil}).Trace("after call: d.QueryMissingTimestamps")
		if err != nil {
			ilogger.T(ctx).WithError(err).Errorf("consistency check %s %s: query missing", inst.Name, jobType)
			return
		}
		if len(missing) == 0 {
			ilogger.T(ctx).WithField("fn", "checkConsistencyForJobType").Trace("branch: consistency_no_more_gaps")
			break
		}
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "checkConsistencyForJobType", "instrument": inst.Name, "batch": len(missing)}).Trace("before call: insertBatched")
		insertBatched(ctx, d, inst.ID, jobType, missing, fmt.Sprintf("consistency-check %s %s", inst.Name, jobType))
		ilogger.T(ctx).WithFields(logrus.Fields{"fn": "checkConsistencyForJobType", "instrument": inst.Name}).Trace("after call: insertBatched")
		if len(missing) < consistencyBatchSize {
			break
		}
		cursor = missing[len(missing)-1]
		ilogger.T(ctx).WithField("fn", "checkConsistencyForJobType").Trace("loop: consistency cursor iteration end")
	}
}

// ── SHARED HELPERS ────────────────────────────────────────────────────────────

// jobTypeInterval returns the time step between consecutive slots.
// TICK = 1 hour; CANDLE = 1 day (24 h UTC).
func jobTypeInterval(jobType state.JobType) time.Duration {
	if jobType == state.JobTypeTICK {
		return time.Hour
	}
	return 24 * time.Hour
}

// normalizeTS truncates t to the canonical slot boundary for jobType.
// TICK → top-of-hour; CANDLE → midnight UTC.
func normalizeTS(t time.Time, jobType state.JobType) time.Time {
	if jobType == state.JobTypeCANDLE {
		return t.Truncate(24 * time.Hour)
	}
	return t.Truncate(time.Hour)
}

// insertBatched splits candidates into chunks of maintenanceSeedBatchSize and
// inserts each chunk as a single multi-row INSERT per transaction using
// ON CONFLICT DO NOTHING. One SQL statement per batch replaces the previous
// per-row loop, reducing parse/plan/index overhead by ~720×.
func insertBatched(ctx context.Context, d *dal.DAL, instrumentID uuid.UUID, jobType state.JobType, candidates []time.Time, phase string) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "insertBatched", "phase": phase, "candidates": len(candidates)}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "insertBatched").Trace("fn_exit")

	for i := 0; i < len(candidates) && ctx.Err() == nil; i += maintenanceSeedBatchSize {
		ilogger.T(ctx).WithField("fn", "insertBatched").Trace("loop: insert batch iteration start")
		end := i + maintenanceSeedBatchSize
		if end > len(candidates) {
			end = len(candidates)
		}
		batchIdx := i / maintenanceSeedBatchSize
		batchSize := end - i
		ilogger.T(ctx).WithFields(logrus.Fields{"phase": phase, "batch_idx": batchIdx, "batch_size": batchSize, "offset": i, "total": len(candidates)}).Debug("maintenance: insertBatched batch vars")
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "batch_idx": batchIdx, "size": batchSize}).Trace("before query")
		if err := d.Execute(ctx, func(tx *ent.Tx) error {
			if err := insertBatchPendingOnConflictDoNothing(ctx, tx, instrumentID, jobType, candidates[i:end], time.Now().UTC()); err != nil {
				return fmt.Errorf("%s batch[%d]: %w", phase, batchIdx, err)
			}
			return nil
		}); err != nil {
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "err": true}).Trace("after query")
			ilogger.T(ctx).WithError(err).Errorf("maintenance %s: insert", phase)
		} else {
			ilogger.T(ctx).WithFields(logrus.Fields{"query": "ExecContext", "err": false, "batch_idx": batchIdx}).Trace("after query")
		}
		ilogger.T(ctx).WithField("fn", "insertBatched").Trace("loop: insert batch iteration end")
	}
}

// insertBatchPendingOnConflictDoNothing inserts all timestamps in a single
// multi-row INSERT statement. Shared values (instrument_id, job_type, status,
// updated_at) are bound once as $1–$4 and reused across every tuple; per-row
// params start at $5 (id) and $6 (timestamp), advancing by 2 per row.
// Rows that already exist at (instrument_id, timestamp, job_type) are silently
// skipped via ON CONFLICT DO NOTHING, preserving idempotency.
func insertBatchPendingOnConflictDoNothing(
	ctx context.Context,
	tx *ent.Tx,
	instrumentID uuid.UUID,
	jobType state.JobType,
	timestamps []time.Time,
	updatedAt time.Time,
) error {
	if len(timestamps) == 0 {
		return nil
	}

	// $1=instrumentID $2=jobType $3=status $4=updatedAt (shared across all tuples)
	const sharedCount = 4
	args := make([]any, 0, sharedCount+len(timestamps)*2)
	args = append(args, instrumentID, string(jobType), string(state.StatusPENDING), updatedAt)

	var sb strings.Builder
	sb.WriteString(`INSERT INTO ingestion.states (
		id, instrument_id, job_type, timestamp, status,
		is_holiday, resolved_tick_count, retry_count, not_found_streak,
		is_deleted, updated_at
	) VALUES `)

	for i, ts := range timestamps {
		if i > 0 {
			sb.WriteByte(',')
		}
		idParam := sharedCount + i*2 + 1 // $5, $7, $9, ...
		tsParam := sharedCount + i*2 + 2 // $6, $8, $10, ...
		_, _ = fmt.Fprintf(&sb, "($%d,$1,$2,$%d,$3,false,0,0,0,false,$4)", idParam, tsParam)
		args = append(args, uuid.New(), ts)
	}

	sb.WriteString(` ON CONFLICT (instrument_id, timestamp, job_type) DO NOTHING`)

	return tx.ExecContext(ctx, sb.String(), args...)
}

// insertStatePendingOnConflictDoNothing inserts one PENDING row, silently
// skipping rows that already exist at (instrument_id, timestamp, job_type).
func insertStatePendingOnConflictDoNothing(
	ctx context.Context,
	tx *ent.Tx,
	instrumentID uuid.UUID,
	jobType state.JobType,
	ts time.Time,
	updatedAt time.Time,
) error {
	return tx.ExecContext(ctx, `
		INSERT INTO ingestion.states (
			id, instrument_id, job_type, timestamp, status,
			is_holiday, resolved_tick_count, retry_count, not_found_streak,
			is_deleted, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, false, 0, 0, 0, false, $6)
		ON CONFLICT (instrument_id, timestamp, job_type) DO NOTHING
	`,
		uuid.New(),
		instrumentID,
		string(jobType),
		ts,
		string(state.StatusPENDING),
		updatedAt,
	)
}

// queryStateMinTS returns the earliest normalized timestamp for the given
// instrument/jobType. Returns zero time when no rows exist.
func queryStateMinTS(ctx context.Context, d *dal.DAL, instrumentID uuid.UUID, jobType state.JobType) (time.Time, error) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "queryStateMinTS", "job_type": jobType}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "queryStateMinTS").Trace("fn_exit")

	var minTS time.Time
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.First", "job_type": jobType}).Trace("before query")
	err := d.Execute(ctx, func(tx *ent.Tx) error {
		row, e := tx.State.Query().
			Where(
				state.InstrumentID(instrumentID),
				state.JobTypeEQ(jobType),
				state.IsDeletedEQ(false),
			).
			Order(state.ByTimestamp()).
			First(ctx)
		if e != nil {
			if ent.IsNotFound(e) {
				return nil
			}
			return e
		}
		minTS = normalizeTS(row.Timestamp.UTC(), jobType)
		return nil
	})
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.First", "found": !minTS.IsZero(), "err": err != nil}).Trace("after query")
	return minTS, err
}

// queryStateMaxTS returns the latest normalized timestamp for the given
// instrument/jobType. Returns zero time when no rows exist.
func queryStateMaxTS(ctx context.Context, d *dal.DAL, instrumentID uuid.UUID, jobType state.JobType) (time.Time, error) {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "queryStateMaxTS", "job_type": jobType}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "queryStateMaxTS").Trace("fn_exit")

	var maxTS time.Time
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.First", "job_type": jobType, "order": "DESC"}).Trace("before query")
	err := d.Execute(ctx, func(tx *ent.Tx) error {
		row, e := tx.State.Query().
			Where(
				state.InstrumentID(instrumentID),
				state.JobTypeEQ(jobType),
				state.IsDeletedEQ(false),
			).
			Order(state.ByTimestamp(sql.OrderDesc())).
			First(ctx)
		if e != nil {
			if ent.IsNotFound(e) {
				return nil
			}
			return e
		}
		maxTS = normalizeTS(row.Timestamp.UTC(), jobType)
		return nil
	})
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.First", "found": !maxTS.IsZero(), "err": err != nil}).Trace("after query")
	return maxTS, err
}
