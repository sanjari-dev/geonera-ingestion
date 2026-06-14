package dal

import (
	"context"
	"fmt"

	"github.com/sanjari-dev/geonera-ingestion/ent"
)

// DAL wraps the ent.Client and exposes transactional helpers for ETL workers.
type DAL struct {
	db *ent.Client
}

// New constructs a DAL from a single PostgreSQL ent.Client.
func New(db *ent.Client) *DAL {
	return &DAL{db: db}
}

// Execute runs fn inside a single PostgreSQL transaction.
// The transaction is automatically rolled back if fn returns an error.
func (d *DAL) Execute(ctx context.Context, fn func(tx *ent.Tx) error) error {
	tx, err := d.db.Tx(ctx)
	if err != nil {
		return fmt.Errorf("dal: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
