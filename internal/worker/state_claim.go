package worker

import (
	"context"
	"time"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/ent/state"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
)

func claimStateAsProcessed(ctx context.Context, d *dal.DAL, configure func(*ent.StateQuery) *ent.StateQuery) (*ent.State, error) {
	claimed, _, err := claimStateWithPrevious(ctx, d, configure, func(row *ent.State) (state.PreviousStatus, state.Status) {
		return state.PreviousStatus(row.Status), state.StatusPROCESSED
	})
	return claimed, err
}

func claimStateWithPrevious(
	ctx context.Context,
	d *dal.DAL,
	configure func(*ent.StateQuery) *ent.StateQuery,
	transition func(*ent.State) (state.PreviousStatus, state.Status),
) (*ent.State, *state.PreviousStatus, error) {
	var claimed *ent.State
	var preclaimPrevious *state.PreviousStatus
	err := d.Execute(ctx, func(tx *ent.Tx) error {
		row, err := configure(tx.State.Query()).FirstForUpdateSkipLocked(ctx)
		if err != nil {
			return err
		}
		preclaimPrevious = row.PreviousStatus
		previousStatus, nextStatus := transition(row)
		claimed, err = tx.State.UpdateOneID(row.ID).
			SetPreviousStatus(previousStatus).
			SetStatus(nextStatus).
			SetUpdatedAt(time.Now().UTC()).
			Save(ctx)
		if err != nil {
			return err
		}
		claimed.Edges = row.Edges
		return nil
	})
	return claimed, preclaimPrevious, err
}
