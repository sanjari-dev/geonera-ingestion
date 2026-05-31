package worker

import (
	"errors"

	"github.com/sanjari-dev/geonera-ingestion/internal/dukascopy"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
)

// errNotImplemented is returned by external operations whose client code has
// not yet been wired up (e.g. candle Parquet builder, candle R2 paths).
// Claimed rows will land in FAILED (recoverable by Backfill).
var errNotImplemented = errors.New("not implemented")

// Package-level singleton clients shared by all tick (and candle) workers.
// Both must be set via InitClients before any worker goroutine is dispatched.
var (
	r2Client  *r2.Client
	dukClient *dukascopy.Client
)

// errClientsNotInitialized is returned by any external operation that is
// invoked before InitClients has been called.
var errClientsNotInitialized = errors.New(
	"worker: external clients not initialised — call worker.InitClients at startup",
)

// InitClients wires up the Cloudflare R2 bucket client and the Dukascopy
// HTTP client.  It must be called once during application startup, before
// the HTTP server and MQ consumers are started.
func InitClients(r2c *r2.Client, dc *dukascopy.Client) {
	r2Client = r2c
	dukClient = dc
}
