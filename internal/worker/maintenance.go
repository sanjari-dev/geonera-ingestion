package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"entgo.io/ent/dialect/sql"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/instrument"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
)

var maintenanceTracer = otel.Tracer("worker/maintenance")

// errR2NotFound mirrors r2.ErrNotFound so that the pruning sweep can treat a
// missing object as "already gone" and proceed to hard-delete the DB row.
var errR2NotFound = r2.ErrNotFound

func isR2NotFound(err error) bool { return errors.Is(err, r2.ErrNotFound) }

// deleteTickParquetFromR2 removes the hourly Tick Parquet file for this state
// row from Cloudflare R2.
//
// Path: ingestion/dukascopy/ticks/{instrument}/{YYYY}/{MM}/ticks-{instrument}-{YYYY-MM-DD}-{HH}.parquet
//
// Returns r2.ErrNotFound when the object is already absent — the sweep treats
// that as "done" and proceeds to hard-delete the DB row.
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
//
// Path: ingestion/dukascopy/candles/{instrument}/{YYYY}/candles-{instrument}-{YYYY-MM-DD}.parquet
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

func RunMaintenanceHandler(ctx context.Context, d *dal.DAL) {
	ctx, span := maintenanceTracer.Start(ctx, "RunMaintenanceHandler")
	defer span.End()

	tx, err := d.AcquireAdvisoryLock(ctx)
	if err != nil {
		span.RecordError(err)
		log.Printf("maintenance: acquire lock (traceID=%s): %v", span.SpanContext().TraceID(), err)
		return
	}
	defer func() { _ = tx.Rollback() }()

	var locked bool
	err = tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", dal.LockIDMaintenance).Scan(&locked)
	if err != nil || !locked {
		span.AddEvent("LockIDMaintenance already held by another instance, skipping")
		return
	}

	lockCtx, cancelETL := context.WithCancel(ctx)
	defer cancelETL()

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-lockCtx.Done():
				return
			case <-ticker.C:
				hbCtx, hbCancel := context.WithTimeout(lockCtx, 5*time.Second)
				var alive bool
				err := tx.QueryRowContext(hbCtx, "SELECT true").Scan(&alive)
				hbCancel()
				if err != nil {
					span.RecordError(fmt.Errorf("maintenance lock heartbeat lost: %w", err))
					cancelETL()
					return
				}
			}
		}
	}()

	runForwardSeeding(lockCtx, d)
	runHistoricalGapFill(lockCtx, d)
	runPruning(lockCtx, d)
}

// ── Forward Seeding ───────────────────────────────────────────────────────────

func runForwardSeeding(ctx context.Context, d *dal.DAL) {
	defer recoverGoroutine(ctx, "runForwardSeeding")

	ctx, span := maintenanceTracer.Start(ctx, "maintenance/forward-seeding")
	defer span.End()

	var instruments []*ent.Instrument
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		var e error
		instruments, e = tx.Instrument.Query().
			Where(instrument.IsActiveEQ(true)).
			All(ctx)
		return e
	})
	if err != nil {
		span.RecordError(err)
		log.Printf("forward seeding: query instruments (traceID=%s): %v", span.SpanContext().TraceID(), err)
		return
	}

	for _, inst := range instruments {
		if ctx.Err() != nil {
			return
		}
		if inst.StartDate == nil {
			continue
		}
		seedForwardForInstrument(ctx, d, inst, state.JobTypeTICK)
		seedForwardForInstrument(ctx, d, inst, state.JobTypeCANDLE)
	}
}

// seedForwardForInstrument seeds new PENDING rows from max+interval up to
// min(max+30d, now) for the given instrument and job type.
// All inserts for a batch are done in a single transaction.
func seedForwardForInstrument(ctx context.Context, d *dal.DAL, inst *ent.Instrument, jobType state.JobType) {
	_, span := maintenanceTracer.Start(ctx, "maintenance/forward-seeding/instrument",
		trace.WithAttributes(
			attribute.String("instrument.name", inst.Name),
			attribute.String("job.type", string(jobType)),
		),
	)
	defer span.End()

	var interval time.Duration
	if jobType == state.JobTypeTICK {
		interval = time.Hour
	} else {
		interval = 24 * time.Hour // CANDLE = 1 day
	}

	// Find MAX(timestamp) for this instrument+jobType.
	// If no rows exist yet, start from StartDate.
	var maxTS time.Time
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
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
				maxTS = inst.StartDate.UTC()
				return nil
			}
			return e
		}
		maxTS = row.Timestamp.UTC()
		return nil
	})
	if err != nil {
		span.RecordError(err)
		log.Printf("forward seeding %s %s: query max (traceID=%s): %v", inst.Name, jobType, span.SpanContext().TraceID(), err)
		return
	}

	now := time.Now().UTC()

	// For CANDLE: normalize maxTS to midnight.
	if jobType == state.JobTypeCANDLE {
		maxTS = maxTS.Truncate(24 * time.Hour)
	}

	// Determine seed range end: min(max+30d, now).
	// TICK: 30d = 720 hours; CANDLE: 30 rows = 30 days.
	rangeEnd := maxTS.Add(30 * 24 * time.Hour)
	if rangeEnd.After(now) {
		rangeEnd = now
	}
	if jobType == state.JobTypeCANDLE {
		rangeEnd = rangeEnd.Truncate(24 * time.Hour)
	}

	// Build candidate timestamps.
	var candidates []time.Time
	for ts := maxTS.Add(interval); !ts.After(rangeEnd); ts = ts.Add(interval) {
		if jobType == state.JobTypeCANDLE {
			ts = ts.Truncate(24 * time.Hour)
		}
		candidates = append(candidates, ts)
	}
	if len(candidates) == 0 {
		return
	}

	// Query existing timestamps in range, then insert only missing ones.
	err = d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		rangeStart := candidates[0]
		rangeLast := candidates[len(candidates)-1]

		existing, e := tx.State.Query().
			Where(
				state.InstrumentID(inst.ID),
				state.JobTypeEQ(jobType),
				state.TimestampGTE(rangeStart),
				state.TimestampLTE(rangeLast),
				state.IsDeletedEQ(false),
			).
			All(ctx)
		if e != nil {
			return e
		}

		existingSet := make(map[time.Time]struct{}, len(existing))
		for _, r := range existing {
			existingSet[r.Timestamp.UTC()] = struct{}{}
		}

		insertedAt := time.Now().UTC()
		for _, ts := range candidates {
			if _, ok := existingSet[ts]; ok {
				continue
			}
			e = tx.State.Create().
				SetInstrumentID(inst.ID).
				SetJobType(jobType).
				SetTimestamp(ts).
				SetStatus(state.StatusPENDING).
				SetUpdatedAt(insertedAt).
				Exec(ctx)
			if e != nil {
				return fmt.Errorf("seed forward insert %s %s %s: %w", inst.Name, jobType, ts, e)
			}
		}
		return nil
	})
	if err != nil {
		span.RecordError(err)
		log.Printf("forward seeding %s %s: insert batch (traceID=%s): %v", inst.Name, jobType, span.SpanContext().TraceID(), err)
	}
}

// ── Historical Gap Fill ───────────────────────────────────────────────────────

func runHistoricalGapFill(ctx context.Context, d *dal.DAL) {
	defer recoverGoroutine(ctx, "runHistoricalGapFill")

	ctx, span := maintenanceTracer.Start(ctx, "maintenance/historical-gap-fill")
	defer span.End()

	var instruments []*ent.Instrument
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		var e error
		instruments, e = tx.Instrument.Query().
			Where(instrument.IsActiveEQ(true)).
			All(ctx)
		return e
	})
	if err != nil {
		span.RecordError(err)
		log.Printf("historical gap fill: query instruments (traceID=%s): %v", span.SpanContext().TraceID(), err)
		return
	}

	for _, inst := range instruments {
		if ctx.Err() != nil {
			return
		}
		if inst.StartDate == nil {
			continue
		}
		gapFillInstrument(ctx, d, inst)
	}
}

// gapFillInstrument performs backward gap fill and IsPause management for one
// active instrument.
func gapFillInstrument(ctx context.Context, d *dal.DAL, inst *ent.Instrument) {
	_, span := maintenanceTracer.Start(ctx, "maintenance/historical-gap-fill/instrument",
		trace.WithAttributes(
			attribute.String("instrument.name", inst.Name),
		),
	)
	defer span.End()

	startDate := inst.StartDate.UTC()

	// Find MIN(timestamp) WHERE job_type=TICK AND status=CONFIRMED.
	var minConfirmedTS *time.Time
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		row, e := tx.State.Query().
			Where(
				state.InstrumentID(inst.ID),
				state.JobTypeEQ(state.JobTypeTICK),
				state.StatusEQ(state.StatusCONFIRMED),
				state.IsDeletedEQ(false),
			).
			Order(state.ByTimestamp()).
			First(ctx)
		if e != nil {
			if ent.IsNotFound(e) {
				// No CONFIRMED rows yet — instrument still being seeded forward.
				return nil
			}
			return e
		}
		ts := row.Timestamp.UTC()
		minConfirmedTS = &ts
		return nil
	})
	if err != nil {
		span.RecordError(err)
		log.Printf("gap fill %s: query min confirmed (traceID=%s): %v", inst.Name, span.SpanContext().TraceID(), err)
		return
	}

	// No CONFIRMED rows yet: skip IsPause logic entirely.
	if minConfirmedTS == nil {
		return
	}

	// If MIN > StartDate: seed rows backward from MIN-1h down to max(MIN-30d, startDate).
	if minConfirmedTS.After(startDate) {
		seedBackwardForInstrument(ctx, d, inst, *minConfirmedTS)
	}

	// Run isBackfillComplete check: count stuck rows between [startDate, minConfirmedTS).
	hasStuck, err := isBackfillComplete(ctx, d, inst.ID, startDate, *minConfirmedTS)
	if err != nil {
		span.RecordError(err)
		log.Printf("gap fill %s: isBackfillComplete (traceID=%s): %v", inst.Name, span.SpanContext().TraceID(), err)
		return
	}

	if hasStuck {
		if e := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			_, e := tx.Instrument.UpdateOneID(inst.ID).SetIsPause(true).Save(ctx)
			return e
		}); e != nil {
			span.RecordError(e)
			log.Printf("gap fill %s: set IsPause=true (traceID=%s): %v", inst.Name, span.SpanContext().TraceID(), e)
		}
		return
	}

	// If MIN <= StartDate: backfill fully complete — set IsPause=false.
	if !minConfirmedTS.After(startDate) {
		if e := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			_, e := tx.Instrument.UpdateOneID(inst.ID).SetIsPause(false).Save(ctx)
			return e
		}); e != nil {
			span.RecordError(e)
			log.Printf("gap fill %s: set IsPause=false (traceID=%s): %v", inst.Name, span.SpanContext().TraceID(), e)
		}
	}
}

// seedBackwardForInstrument seeds TICK rows backward from minConfirmedTS-1h
// down to max(minConfirmedTS-30d, startDate).
// All inserts are done in a single transaction, skipping existing timestamps.
func seedBackwardForInstrument(ctx context.Context, d *dal.DAL, inst *ent.Instrument, minConfirmedTS time.Time) {
	span := trace.SpanFromContext(ctx)

	startDate := inst.StartDate.UTC()
	interval := time.Hour

	backTo := minConfirmedTS.Add(-30 * 24 * time.Hour)
	if backTo.Before(startDate) {
		backTo = startDate
	}

	// Build candidates descending: minConfirmedTS-1h, -2h, ... down to backTo.
	var candidates []time.Time
	for ts := minConfirmedTS.Add(-interval); !ts.Before(backTo); ts = ts.Add(-interval) {
		candidates = append(candidates, ts)
	}
	if len(candidates) == 0 {
		return
	}

	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		// Candidates are in descending order; rangeStart is the last, rangeLast is the first.
		rangeLast := candidates[0]
		rangeStart := candidates[len(candidates)-1]

		existing, e := tx.State.Query().
			Where(
				state.InstrumentID(inst.ID),
				state.JobTypeEQ(state.JobTypeTICK),
				state.TimestampGTE(rangeStart),
				state.TimestampLTE(rangeLast),
				state.IsDeletedEQ(false),
			).
			All(ctx)
		if e != nil {
			return e
		}

		existingSet := make(map[time.Time]struct{}, len(existing))
		for _, r := range existing {
			existingSet[r.Timestamp.UTC()] = struct{}{}
		}

		insertedAt := time.Now().UTC()
		for _, ts := range candidates {
			if _, ok := existingSet[ts]; ok {
				continue
			}
			e = tx.State.Create().
				SetInstrumentID(inst.ID).
				SetJobType(state.JobTypeTICK).
				SetTimestamp(ts).
				SetStatus(state.StatusPENDING).
				SetUpdatedAt(insertedAt).
				Exec(ctx)
			if e != nil {
				return fmt.Errorf("seed backward insert %s %s: %w", inst.Name, ts, e)
			}
		}
		return nil
	})
	if err != nil {
		span.RecordError(err)
		log.Printf("gap fill %s: backward seed (traceID=%s): %v", inst.Name, span.SpanContext().TraceID(), err)
	}
}

// isBackfillComplete checks whether there are stuck rows between [startDate, minConfirmedTS).
// Returns (hasStuck=true, nil) if any stuck rows exist.
//
// Stuck row definition:
//   - Status IN (FAILED, BROKEN, ABANDONED)
//   - OR: Status IN (PENDING, PROCESSED) AND UpdatedAt < now()-24h
//   - EXCLUDE: NOT_FOUND with NotFoundStreak < 3 (valid waiting cycle)
func isBackfillComplete(ctx context.Context, d *dal.DAL, instrumentID uuid.UUID, startDate, minConfirmedTS time.Time) (bool, error) {
	staleThreshold := time.Now().UTC().Add(-24 * time.Hour)

	var stuckCount int
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		count, e := tx.State.Query().
			Where(
				state.InstrumentID(instrumentID),
				state.JobTypeEQ(state.JobTypeTICK),
				state.TimestampGTE(startDate),
				state.TimestampLT(minConfirmedTS),
				state.IsDeletedEQ(false),
				state.And(
					// Matches any stuck row variant.
					state.Or(
						// Hard-stuck: terminal failure statuses.
						state.StatusIn(
							state.StatusFAILED,
							state.StatusBROKEN,
							state.StatusABANDONED,
						),
						// Soft-stuck: in-progress but stale for > 24h.
						state.And(
							state.StatusIn(
								state.StatusPENDING,
								state.StatusPROCESSED,
							),
							state.UpdatedAtLT(staleThreshold),
						),
					),
					// Exclude NOT_FOUND rows with streak < 3 (valid waiting cycle).
					state.Not(
						state.And(
							state.StatusEQ(state.StatusNOT_FOUND),
							state.NotFoundStreakLT(3),
						),
					),
				),
			).
			Count(ctx)
		if e != nil {
			return e
		}
		stuckCount = count
		return nil
	})
	if err != nil {
		return false, err
	}
	return stuckCount > 0, nil
}

// ── Pruning (2-Phase Mark & Sweep) ───────────────────────────────────────────

func runPruning(ctx context.Context, d *dal.DAL) {
	defer recoverGoroutine(ctx, "runPruning")

	ctx, span := maintenanceTracer.Start(ctx, "maintenance/pruning")
	defer span.End()

	runPruningPhase1Mark(ctx, d)
	runPruningPhase2Sweep(ctx, d)
}

// runPruningPhase1Mark soft-deletes rows WHERE timestamp < startDate AND
// is_deleted=false for each active instrument, in batches of 100.
func runPruningPhase1Mark(ctx context.Context, d *dal.DAL) {
	_, span := maintenanceTracer.Start(ctx, "maintenance/pruning/phase1-mark")
	defer span.End()

	var instruments []*ent.Instrument
	err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
		var e error
		instruments, e = tx.Instrument.Query().
			Where(instrument.IsActiveEQ(true)).
			All(ctx)
		return e
	})
	if err != nil {
		span.RecordError(err)
		log.Printf("pruning phase1: query instruments (traceID=%s): %v", span.SpanContext().TraceID(), err)
		return
	}

	for _, inst := range instruments {
		if ctx.Err() != nil {
			return
		}
		if inst.StartDate == nil {
			continue
		}
		startDate := *inst.StartDate

		// Batch loop: keep marking until no rows remain below startDate.
		for ctx.Err() == nil {
			var batchLen int
			err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
				rows, e := tx.State.Query().
					Where(
						state.InstrumentID(inst.ID),
						state.TimestampLT(startDate),
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
			})
			if err != nil {
				span.RecordError(err)
				log.Printf("pruning phase1 mark %s (traceID=%s): %v", inst.Name, span.SpanContext().TraceID(), err)
				break
			}
			if batchLen == 0 {
				break
			}
		}
	}
}

// runPruningPhase2Sweep hard-deletes is_deleted=true rows in batches of 50.
//
// For each row the corresponding R2 object is deleted first (using the correct
// path for the row's job_type: TICK → TickObjectKey, CANDLE → CandleObjectKey).
// Only on R2 success or NoSuchKey does the sweep proceed to hard-delete the
// DB row.  On any other R2 error the row is left with is_deleted=true for
// idempotent retry in the next maintenance cycle.
func runPruningPhase2Sweep(ctx context.Context, d *dal.DAL) {
	_, span := maintenanceTracer.Start(ctx, "maintenance/pruning/phase2-sweep")
	defer span.End()

	for ctx.Err() == nil {
		var batch []*ent.State
		err := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
			var e error
			// WithInstrument is required: deleteTickParquetFromR2 and
			// deleteCandleParquetFromR2 both need row.Edges.Instrument.Name
			// to compute the canonical R2 object key.
			batch, e = tx.State.Query().
				Where(state.IsDeletedEQ(true)).
				WithInstrument().
				Limit(50).
				All(ctx)
			return e
		})
		if err != nil {
			span.RecordError(err)
			log.Printf("pruning phase2: query batch (traceID=%s): %v", span.SpanContext().TraceID(), err)
			break
		}
		if len(batch) == 0 {
			break
		}

		for _, row := range batch {
			if ctx.Err() != nil {
				return
			}

			// Branch on job type to use the correct R2 path.
			var r2Err error
			switch row.JobType {
			case state.JobTypeTICK:
				r2Err = deleteTickParquetFromR2(ctx, row)
			case state.JobTypeCANDLE:
				r2Err = deleteCandleParquetFromR2(ctx, row)
			default:
				// Unknown job type — no R2 object to delete; proceed directly
				// to hard-delete so the DB row does not stay orphaned.
			}

			if r2Err != nil && !isR2NotFound(r2Err) {
				// Transient R2 error — leave is_deleted=true for next cycle.
				span.RecordError(r2Err)
				log.Printf("pruning phase2: R2 delete %s (%s) (traceID=%s): %v (will retry next cycle)",
					row.ID, row.JobType, span.SpanContext().TraceID(), r2Err)
				continue
			}
			// R2 succeeded or returned NoSuchKey — hard-delete from DB.
			if e := d.ExecuteInPool(ctx, func(tx *ent.Tx) error {
				return tx.State.DeleteOneID(row.ID).Exec(ctx)
			}); e != nil {
				span.RecordError(e)
				log.Printf("pruning phase2: hard-delete %s (traceID=%s): %v", row.ID, span.SpanContext().TraceID(), e)
			}
		}
	}
}
