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
		preclaimPrevious = row.PreviousStatus
		previousStatus, nextStatus := transition(row)
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
