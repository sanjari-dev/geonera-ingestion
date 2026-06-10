package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"entgo.io/ent/dialect/sql"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
)

var maintenanceTracer = otel.Tracer("worker/maintenance")

var errR2NotFound = r2.ErrNotFound

func isR2NotFound(err error) bool { return errors.Is(err, r2.ErrNotFound) }

// maintenanceSeedBatchSize is the maximum number of rows inserted per DB
// transaction. 720 = 30 days × 24 h for TICK; for CANDLE the same limit
// covers ~2 years. Keeps individual transactions short to avoid lock contention
// and statement timeouts.
const maintenanceSeedBatchSize = 720

// deleteTickParquetFromR2 removes the hourly Tick Parquet file for this state
// row from Cloudflare R2.
func deleteTickParquetFromR2(ctx context.Context, row *ent.State) error {
	if r2Client == nil {
		return errClientsNotInitialized
	}
	if row.Edges.Instrument == nil {
		return fmt.Errorf("deleteTickParquetFromR2: instrument edge not loaded for state %s", row.ID)
	}
	key := r2.TickObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	return r2Client.DeleteObject(ctx, key)
}

// deleteCandleParquetFromR2 removes the daily Candle Parquet file for this
// state row from Cloudflare R2.
func deleteCandleParquetFromR2(ctx context.Context, row *ent.State) error {
	if r2Client == nil {
		return errClientsNotInitialized
	}
	if row.Edges.Instrument == nil {
		return fmt.Errorf("deleteCandleParquetFromR2: instrument edge not loaded for state %s", row.ID)
	}
	key := r2.CandleObjectKey(row.Edges.Instrument.Name, row.Timestamp)
	return r2Client.DeleteObject(ctx, key)
}

// ── RunMaintenanceHandler ─────────────────────────────────────────────────────

// RunMaintenanceHandler acquires LockIDMaintenance and iterates over every
// instrument (active and inactive). Per instrument it either bootstraps a
// brand-new states table (Group 1) or runs the four normal-maintenance phases
// A/B/C/D in parallel (Group 2).
func RunMaintenanceHandler(ctx context.Context, d *dal.DAL, onStarted func()) bool {
	ctx, span := maintenanceTracer.Start(ctx, "RunMaintenanceHandler")
	defer span.End()

	return runWithLocks(ctx, d, "maintenance", []int64{dal.LockIDMaintenance}, onStarted, func(lockCtx context.Context) {
		log.Printf("maintenance: lock acquired, running!")

		var instruments []*ent.Instrument
		if err := d.ExecuteInPool(lockCtx, func(tx *ent.Tx) error {
			var e error
			// Fetch ALL instruments — active and inactive — so bootstrap can
			// proceed even before an engineer sets IsActive=true.
			instruments, e = tx.Instrument.Query().All(lockCtx)
			return e
		}); err != nil {
			span.RecordError(err)
			log.Printf("maintenance: query instruments (traceID=%s): %v", span.SpanContext().TraceID(), err)
			return
		}

		for _, inst := range instruments {
			if lockCtx.Err() != nil {
				return
			}
			runMaintenanceForInstrument(lockCtx, d, inst)
		}
	})
}

// runMaintenanceForInstrument dispatches Group 1 (bootstrap) when the states
// table is empty for this instrument, or Group 2 (A + B + C + D in parallel)
// when rows already exist.
func runMaintenanceForInstrument(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	ctx, span := maintenanceTracer.Start(ctx, "maintenance/instrument",
		trace.WithAttributes(attribute.String("instrument.name", inst.Name)))
	defer span.End()

	empty, err := isStatesEmpty(ctx, d, inst.ID)
	if err != nil {
		span.RecordError(err)
		log.Printf("maintenance %s: check empty (traceID=%s): %v", inst.Name, span.SpanContext().TraceID(), err)
		return
	}

	if empty {
		runBootstrap(ctx, d, inst)
		return
	}

	// Group 2: A + B + C + D run concurrently for this instrument.
	var wg sync.WaitGroup
	wg.Add(4)
	go func() { defer wg.Done(); runForwardSeeding(ctx, d, inst) }()
	go func() { defer wg.Done(); runBackwardGapFill(ctx, d, inst) }()
	go func() { defer wg.Done(); runPruning(ctx, d, inst) }()
	go func() { defer wg.Done(); runConsistencyCheck(ctx, d, inst) }()
	wg.Wait()
}

func isStatesEmpty(ctx context.Context, d *dal.DAL, instrumentID uuid.UUID) (bool, error) {
	var count int
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		var e error
		count, e = tx.State.Query().
			Where(
				state.InstrumentID(instrumentID),
				state.IsDeletedEQ(false),
			).
			Count(ctx)
		return e
	})
	return count == 0, err
}

// ── GROUP 1 — BOOTSTRAP ───────────────────────────────────────────────────────

// runBootstrap seeds the full PENDING row set for a brand-new (or fully-wiped)
// instrument. IsPause is set to true during seeding so that Tick/Candle workers
// skip this instrument while rows are being inserted. IsActive is never touched
// here — that flag is managed exclusively by engineers. All inserts use
// ON CONFLICT DO NOTHING so the function is safe to retry after a partial run.
func runBootstrap(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	defer recoverGoroutine(ctx, "maintenance/bootstrap")

	ctx, span := maintenanceTracer.Start(ctx, "maintenance/bootstrap",
		trace.WithAttributes(attribute.String("instrument.name", inst.Name)))
	defer span.End()

	log.Printf("maintenance bootstrap %s: seeding from %s", inst.Name, inst.StartDate.UTC().Format(time.RFC3339))

	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		_, e := tx.Instrument.UpdateOneID(inst.ID).SetIsPause(true).Save(ctx)
		return e
	}); err != nil {
		span.RecordError(err)
		log.Printf("maintenance bootstrap %s: set IsPause=true (traceID=%s): %v", inst.Name, span.SpanContext().TraceID(), err)
		return
	}

	now := time.Now().UTC()
	for _, jobType := range []state.JobType{state.JobTypeTICK, state.JobTypeCANDLE} {
		if ctx.Err() != nil {
			break
		}
		interval := jobTypeInterval(jobType)
		startNorm := normalizeTS(inst.StartDate.UTC(), jobType)
		nowNorm := normalizeTS(now, jobType)

		var candidates []time.Time
		for ts := startNorm; !ts.After(nowNorm); ts = ts.Add(interval) {
			candidates = append(candidates, ts)
		}
		insertBatched(ctx, d, inst.ID, jobType, candidates, fmt.Sprintf("bootstrap %s", inst.Name))
	}

	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		_, e := tx.Instrument.UpdateOneID(inst.ID).SetIsPause(false).Save(ctx)
		return e
	}); err != nil {
		span.RecordError(err)
		log.Printf("maintenance bootstrap %s: set IsPause=false (traceID=%s): %v", inst.Name, span.SpanContext().TraceID(), err)
		return
	}
	log.Printf("maintenance bootstrap %s: complete", inst.Name)
}

// ── GROUP 2A — FORWARD SEEDING ────────────────────────────────────────────────

// runForwardSeeding inserts PENDING rows from MAX(timestamp)+interval up to
// time.Now() for both TICK and CANDLE, in batches. No upper-window cap —
// the full range is always covered in one maintenance cycle.
func runForwardSeeding(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	defer recoverGoroutine(ctx, "maintenance/forward-seeding")

	ctx, span := maintenanceTracer.Start(ctx, "maintenance/forward-seeding",
		trace.WithAttributes(attribute.String("instrument.name", inst.Name)))
	defer span.End()

	for _, jobType := range []state.JobType{state.JobTypeTICK, state.JobTypeCANDLE} {
		if ctx.Err() != nil {
			return
		}
		interval := jobTypeInterval(jobType)

		var maxTS time.Time
		if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			row, e := tx.State.Query().
				Where(
					state.InstrumentID(inst.ID),
					state.JobTypeEQ(jobType),
					state.IsDeletedEQ(false),
				).
				Order(state.ByTimestamp(sql.OrderDesc())).
				First(ctx)
			if e != nil {
				if ent.IsNotFound(e) {
					maxTS = normalizeTS(inst.StartDate.UTC(), jobType)
					return nil
				}
				return e
			}
			maxTS = normalizeTS(row.Timestamp.UTC(), jobType)
			return nil
		}); err != nil {
			span.RecordError(err)
			log.Printf("forward seeding %s %s: query max (traceID=%s): %v", inst.Name, jobType, span.SpanContext().TraceID(), err)
			continue
		}

		nowNorm := normalizeTS(time.Now().UTC(), jobType)
		if !maxTS.Before(nowNorm) {
			continue
		}

		var candidates []time.Time
		for ts := maxTS.Add(interval); !ts.After(nowNorm); ts = ts.Add(interval) {
			candidates = append(candidates, ts)
		}
		insertBatched(ctx, d, inst.ID, jobType, candidates, fmt.Sprintf("forward-seeding %s %s", inst.Name, jobType))
	}
}

// ── GROUP 2B — BACKWARD GAP FILL ─────────────────────────────────────────────

// runBackwardGapFill inserts PENDING rows from MIN(timestamp)-interval back to
// Instrument.StartDate for both TICK and CANDLE. Status is intentionally
// ignored — only the existence of a row at a given timestamp matters.
// After filling gaps, managePauseFlag checks for hard-stuck TICK rows and
// updates Instrument.IsPause accordingly.
func runBackwardGapFill(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	defer recoverGoroutine(ctx, "maintenance/backward-gap-fill")

	ctx, span := maintenanceTracer.Start(ctx, "maintenance/backward-gap-fill",
		trace.WithAttributes(attribute.String("instrument.name", inst.Name)))
	defer span.End()

	startDate := inst.StartDate.UTC()

	for _, jobType := range []state.JobType{state.JobTypeTICK, state.JobTypeCANDLE} {
		if ctx.Err() != nil {
			return
		}
		interval := jobTypeInterval(jobType)
		startNorm := normalizeTS(startDate, jobType)

		var minTS time.Time
		if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			row, e := tx.State.Query().
				Where(
					state.InstrumentID(inst.ID),
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
		}); err != nil {
			span.RecordError(err)
			log.Printf("backward gap fill %s %s: query min (traceID=%s): %v", inst.Name, jobType, span.SpanContext().TraceID(), err)
			continue
		}

		if minTS.IsZero() || !minTS.After(startNorm) {
			continue
		}

		var candidates []time.Time
		for ts := minTS.Add(-interval); !ts.Before(startNorm); ts = ts.Add(-interval) {
			candidates = append(candidates, ts)
		}
		insertBatched(ctx, d, inst.ID, jobType, candidates, fmt.Sprintf("backward-gap-fill %s %s", inst.Name, jobType))
	}

	managePauseFlag(ctx, d, inst)
}

// managePauseFlag sets Instrument.IsPause based on whether the TICK historical
// range has been fully seeded back to StartDate. It finds the earliest TICK row
// (any status) and compares against Instrument.StartDate:
//   - MIN > StartDate → gaps remain → IsPause = true
//   - MIN <= StartDate → range fully covered → IsPause = false
//
// Status is intentionally ignored — the backward gap fill already inserted
// PENDING rows for every missing slot; whether those rows have been processed
// yet is irrelevant to the pause decision.
func managePauseFlag(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	span := trace.SpanFromContext(ctx)

	var minTS time.Time
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		row, e := tx.State.Query().
			Where(
				state.InstrumentID(inst.ID),
				state.JobTypeEQ(state.JobTypeTICK),
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
		minTS = normalizeTS(row.Timestamp.UTC(), state.JobTypeTICK)
		return nil
	}); err != nil {
		span.RecordError(err)
		log.Printf("manage pause %s: query min tick (traceID=%s): %v", inst.Name, span.SpanContext().TraceID(), err)
		return
	}
	if minTS.IsZero() {
		return
	}

	startNorm := normalizeTS(inst.StartDate.UTC(), state.JobTypeTICK)
	shouldPause := minTS.After(startNorm)

	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		_, e := tx.Instrument.UpdateOneID(inst.ID).SetIsPause(shouldPause).Save(ctx)
		return e
	}); err != nil {
		span.RecordError(err)
		log.Printf("manage pause %s: set IsPause=%v (traceID=%s): %v", inst.Name, shouldPause, span.SpanContext().TraceID(), err)
	}
}

// ── GROUP 2C — PRUNING ────────────────────────────────────────────────────────

// runPruning soft-deletes rows below StartDate (Phase 1), sweeps the
// corresponding R2 objects (Phase 2), then hard-deletes the succeeded group
// from the DB (Phase 3). Both phases are per-instrument and batched.
func runPruning(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	defer recoverGoroutine(ctx, "maintenance/pruning")

	ctx, span := maintenanceTracer.Start(ctx, "maintenance/pruning",
		trace.WithAttributes(attribute.String("instrument.name", inst.Name)))
	defer span.End()

	runPruningPhase1Mark(ctx, d, inst)
	runPruningPhase2Sweep(ctx, d, inst)
}

// runPruningPhase1Mark soft-deletes rows WHERE timestamp < StartDate AND
// is_deleted=false for this instrument, in batches of 100.
func runPruningPhase1Mark(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	_, span := maintenanceTracer.Start(ctx, "maintenance/pruning/phase1-mark",
		trace.WithAttributes(attribute.String("instrument.name", inst.Name)))
	defer span.End()

	for ctx.Err() == nil {
		var batchLen int
		if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			rows, e := tx.State.Query().
				Where(
					state.InstrumentID(inst.ID),
					state.TimestampLT(inst.StartDate),
					state.IsDeletedEQ(false),
				).
				Limit(100).
				All(ctx)
			if e != nil {
				return e
			}
			batchLen = len(rows)
			if batchLen == 0 {
				return nil
			}
			ids := make([]uuid.UUID, batchLen)
			for i, r := range rows {
				ids[i] = r.ID
			}
			return tx.State.Update().
				Where(state.IDIn(ids...)).
				SetIsDeleted(true).
				SetUpdatedAt(time.Now().UTC()).
				Exec(ctx)
		}); err != nil {
			span.RecordError(err)
			log.Printf("pruning phase1 mark %s (traceID=%s): %v", inst.Name, span.SpanContext().TraceID(), err)
			break
		}
		if batchLen == 0 {
			break
		}
	}
}

// runPruningPhase2Sweep implements the two-phase R2 + DB cleanup:
//
// Phase 2 — R2 Sweep: load all is_deleted=true rows, attempt to delete each
// R2 object, and split results into succeeded (deleted or NoSuchKey) vs failed
// (transient R2 error — left is_deleted=true for retry on the next cycle).
//
// Phase 3 — DB Hard Delete: bulk-delete only the succeeded group from the DB.
// Failed rows remain is_deleted=true and are retried automatically.
func runPruningPhase2Sweep(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	_, span := maintenanceTracer.Start(ctx, "maintenance/pruning/phase2-sweep",
		trace.WithAttributes(attribute.String("instrument.name", inst.Name)))
	defer span.End()

	// ── Phase 2: R2 Sweep ────────────────────────────────────────────────────
	// Load all is_deleted=true rows for this instrument. The set is bounded —
	// only rows below Instrument.StartDate are soft-deleted by Phase 1.
	var allRows []*ent.State
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
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
		span.RecordError(err)
		log.Printf("pruning phase2 %s: query IsDeleted rows (traceID=%s): %v", inst.Name, span.SpanContext().TraceID(), err)
		return
	}
	if len(allRows) == 0 {
		return
	}

	var succeeded []uuid.UUID
	for _, row := range allRows {
		if ctx.Err() != nil {
			break
		}
		var r2Err error
		switch row.JobType {
		case state.JobTypeTICK:
			r2Err = deleteTickParquetFromR2(ctx, row)
		case state.JobTypeCANDLE:
			r2Err = deleteCandleParquetFromR2(ctx, row)
		}
		if r2Err != nil && !isR2NotFound(r2Err) {
			span.RecordError(r2Err)
			log.Printf("pruning phase2 %s: R2 delete %s (%s) (traceID=%s): %v (will retry next cycle)",
				inst.Name, row.ID, row.JobType, span.SpanContext().TraceID(), r2Err)
		} else {
			succeeded = append(succeeded, row.ID)
		}
	}

	// ── Phase 3: DB Hard Delete (succeeded group only) ───────────────────────
	if len(succeeded) == 0 {
		return
	}
	const pruneBatch = 500
	for i := 0; i < len(succeeded); i += pruneBatch {
		end := i + pruneBatch
		if end > len(succeeded) {
			end = len(succeeded)
		}
		chunk := succeeded[i:end]
		if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			_, e := tx.State.Delete().Where(state.IDIn(chunk...)).Exec(ctx)
			return e
		}); err != nil {
			span.RecordError(err)
			log.Printf("pruning phase3 %s: hard-delete (traceID=%s): %v", inst.Name, span.SpanContext().TraceID(), err)
		}
	}
}

// ── GROUP 2D — CONSISTENCY CHECK ─────────────────────────────────────────────

// runConsistencyCheck verifies that every timestamp slot between MIN and MAX
// has a row in states. It runs independently for TICK (hourly) and CANDLE
// (daily). Missing slots are inserted as PENDING in batches.
func runConsistencyCheck(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	defer recoverGoroutine(ctx, "maintenance/consistency-check")

	ctx, span := maintenanceTracer.Start(ctx, "maintenance/consistency-check",
		trace.WithAttributes(attribute.String("instrument.name", inst.Name)))
	defer span.End()

	for _, jobType := range []state.JobType{state.JobTypeTICK, state.JobTypeCANDLE} {
		if ctx.Err() != nil {
			return
		}
		checkConsistencyForJobType(ctx, d, inst, jobType)
	}
}

func checkConsistencyForJobType(ctx context.Context, d *dal.DAL, inst *ent.Instrument, jobType state.JobType) {
	span := trace.SpanFromContext(ctx)
	interval := jobTypeInterval(jobType)

	var minTS, maxTS time.Time
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		rowMin, e := tx.State.Query().
			Where(
				state.InstrumentID(inst.ID),
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
		minTS = normalizeTS(rowMin.Timestamp.UTC(), jobType)

		rowMax, e := tx.State.Query().
			Where(
				state.InstrumentID(inst.ID),
				state.JobTypeEQ(jobType),
				state.IsDeletedEQ(false),
			).
			Order(state.ByTimestamp(sql.OrderDesc())).
			First(ctx)
		if e != nil {
			return e
		}
		maxTS = normalizeTS(rowMax.Timestamp.UTC(), jobType)
		return nil
	}); err != nil {
		span.RecordError(err)
		log.Printf("consistency check %s %s: query min/max (traceID=%s): %v", inst.Name, jobType, span.SpanContext().TraceID(), err)
		return
	}
	if minTS.IsZero() {
		return
	}

	expected := int(maxTS.Sub(minTS)/interval) + 1

	var actual int
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
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
		span.RecordError(err)
		log.Printf("consistency check %s %s: count actual (traceID=%s): %v", inst.Name, jobType, span.SpanContext().TraceID(), err)
		return
	}

	if actual >= expected {
		return
	}

	log.Printf("consistency check %s %s: expected=%d actual=%d — inserting %d missing rows",
		inst.Name, jobType, expected, actual, expected-actual)

	// Fetch all existing timestamps in the [minTS, maxTS] range to compute the
	// set difference in memory. This is efficient for typical ranges (years of
	// data) because we only transfer timestamps, not full row payloads.
	var existingRows []*ent.State
	if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		var e error
		existingRows, e = tx.State.Query().
			Where(
				state.InstrumentID(inst.ID),
				state.JobTypeEQ(jobType),
				state.TimestampGTE(minTS),
				state.TimestampLTE(maxTS),
				state.IsDeletedEQ(false),
			).
			All(ctx)
		return e
	}); err != nil {
		span.RecordError(err)
		log.Printf("consistency check %s %s: query existing (traceID=%s): %v", inst.Name, jobType, span.SpanContext().TraceID(), err)
		return
	}

	existingSet := make(map[time.Time]struct{}, len(existingRows))
	for _, row := range existingRows {
		existingSet[normalizeTS(row.Timestamp.UTC(), jobType)] = struct{}{}
	}

	var missing []time.Time
	for ts := minTS; !ts.After(maxTS); ts = ts.Add(interval) {
		if _, ok := existingSet[ts]; !ok {
			missing = append(missing, ts)
		}
	}
	if len(missing) == 0 {
		return
	}

	sort.Slice(missing, func(i, j int) bool { return missing[i].Before(missing[j]) })
	insertBatched(ctx, d, inst.ID, jobType, missing, fmt.Sprintf("consistency-check %s %s", inst.Name, jobType))
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
// inserts each chunk in a single pool transaction using ON CONFLICT DO NOTHING.
func insertBatched(ctx context.Context, d *dal.DAL, instrumentID uuid.UUID, jobType state.JobType, candidates []time.Time, phase string) {
	span := trace.SpanFromContext(ctx)
	for i := 0; i < len(candidates) && ctx.Err() == nil; i += maintenanceSeedBatchSize {
		end := i + maintenanceSeedBatchSize
		if end > len(candidates) {
			end = len(candidates)
		}
		if err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			insertedAt := time.Now().UTC()
			for _, ts := range candidates[i:end] {
				if err := insertStatePendingOnConflictDoNothing(ctx, tx, instrumentID, jobType, ts, insertedAt); err != nil {
					return fmt.Errorf("%s batch[%d]: %w", phase, i/maintenanceSeedBatchSize, err)
				}
			}
			return nil
		}); err != nil {
			span.RecordError(err)
			log.Printf("maintenance %s: insert (traceID=%s): %v", phase, span.SpanContext().TraceID(), err)
		}
	}
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

