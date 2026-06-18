package worker

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	ilogger "github.com/sanjari-dev/geonera-ingestion/internal/logger"
)

// applyStateClaimUpdate transitions row to newStatus/newPrev inside an existing
// transaction, copies edge data, and emits surrounding Trace logs.
// layer is any so the helper works for both tick and candle backfill layer types.
func applyStateClaimUpdate(ctx context.Context, tx *ent.Tx, row *ent.State, newPrev state.PreviousStatus, newStatus state.Status, layer any) (*ent.State, error) {
	ilogger.T(ctx).WithFields(logrus.Fields{
		"query": "State.UpdateOneID.Save", "instrument": instrName(row), "new_status": newStatus,
	}).Trace("before query")
	updated, err := tx.State.UpdateOneID(row.ID).
		SetPreviousStatus(newPrev).
		SetStatus(newStatus).
		SetUpdatedAt(time.Now().UTC()).
		Save(ctx)
	if err != nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": true}).Trace("after query")
		return nil, err
	}
	ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": false, "layer": layer}).Trace("after query")
	updated.Edges = row.Edges
	return updated, nil
}

func claimStateAsProcessed(ctx context.Context, d *dal.DAL, configure func(*ent.StateQuery) *ent.StateQuery) (*ent.State, error) {
	ilogger.T(ctx).WithField("fn", "claimStateAsProcessed").Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "claimStateAsProcessed").Trace("fn_exit")

	ilogger.T(ctx).Trace("before call: claimStateWithPrevious")
	claimed, _, err := claimStateWithPrevious(ctx, d, configure, func(row *ent.State) (state.PreviousStatus, state.Status) {
		return state.PreviousStatus(row.Status), state.StatusPROCESSED
	})
	ilogger.T(ctx).WithFields(logrus.Fields{"found": claimed != nil, "err": err != nil}).Trace("after call: claimStateWithPrevious")
	return claimed, err
}

func claimStateWithPrevious(
	ctx context.Context,
	d *dal.DAL,
	configure func(*ent.StateQuery) *ent.StateQuery,
	transition func(*ent.State) (state.PreviousStatus, state.Status),
) (*ent.State, *state.PreviousStatus, error) {
	ilogger.T(ctx).WithField("fn", "claimStateWithPrevious").Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "claimStateWithPrevious").Trace("fn_exit")

	var claimed *ent.State
	var preclaimPrevious *state.PreviousStatus
	ilogger.T(ctx).Trace("before call: d.Execute (claim tx)")
	err := d.Execute(ctx, func(tx *ent.Tx) error {
		ilogger.T(ctx).WithField("query", "State.Query.FirstForUpdateSkipLocked").Trace("before query")
		row, err := configure(tx.State.Query()).FirstForUpdateSkipLocked(ctx)
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.Query.FirstForUpdateSkipLocked", "found": err == nil}).Trace("after query")
		if err != nil {
			return err
		}
		ilogger.T(ctx).WithFields(logrus.Fields{
			"state_id": row.ID, "instrument": instrName(row), "ts": row.Timestamp.Format(time.RFC3339),
			"status": row.Status, "prev_status": prevStatusStr(row.PreviousStatus),
			"retry_count": row.RetryCount, "streak": row.NotFoundStreak,
		}).Debug("state_claim: row fields")
		preclaimPrevious = row.PreviousStatus
		previousStatus, nextStatus := transition(row)
		ilogger.T(ctx).WithFields(logrus.Fields{
			"state_id": row.ID, "instrument": instrName(row), "prev": previousStatus, "next": nextStatus,
		}).Debug("state_claim: transition values")
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "state_id": row.ID, "prev": previousStatus, "next": nextStatus}).Trace("before query")
		claimed, err = tx.State.UpdateOneID(row.ID).
			SetPreviousStatus(previousStatus).
			SetStatus(nextStatus).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		ilogger.T(ctx).WithFields(logrus.Fields{"query": "State.UpdateOneID.Save", "err": err != nil}).Trace("after query")
		if err != nil {
			return err
		}
		claimed.Edges = row.Edges
		return nil
	})
	ilogger.T(ctx).WithFields(logrus.Fields{"found": claimed != nil, "err": err != nil}).Trace("after call: d.Execute (claim tx)")
	return claimed, preclaimPrevious, err
}
