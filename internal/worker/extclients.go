package worker

import (
	"errors"
	"os"
	"strconv"

	"github.com/sirupsen/logrus"

	"github.com/sanjari-dev/geonera-ingestion/internal/activitylog"
	"github.com/sanjari-dev/geonera-ingestion/internal/dukascopy"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
)

// Package-level singleton clients shared by all tick (and candle) workers.
// Both must be set via InitClients before any worker goroutine is dispatched.
var (
	r2Client  *r2.Client
	dukClient *dukascopy.Client
	actLogger *activitylog.Logger
)

// errClientsNotInitialized is returned by any external operation
// invoked before InitClients has been called.
var errClientsNotInitialized = errors.New(
	"worker: external clients not initialised — call worker.InitClients at startup",
)

// InitClients wires up the Cloudflare R2 bucket client and the Dukascopy
// HTTP client and starts the download rate limiter goroutine.
// Must be called once at startup, before the HTTP server and MQ consumers are started.
func InitClients(r2c *r2.Client, dc *dukascopy.Client) {
	r2Client = r2c
	dukClient = dc
	initBackfillConfig()
	InitDownloadRateLimiter()
}

// initBackfillConfig reads BACKFILL_CLAIM_LIMIT from the environment and
// overrides the default backfillMasterClaimLimit. Must be called before
// startDownloadWorkers so the backfill queue channel is sized correctly.
func initBackfillConfig() {
	v := os.Getenv("BACKFILL_CLAIM_LIMIT")
	if v == "" {
		return
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		logrus.WithFields(logrus.Fields{"value": v, "default": backfillMasterClaimLimit}).Warn("worker: invalid BACKFILL_CLAIM_LIMIT (must be a positive integer) — using default")
		return
	}
	backfillMasterClaimLimit = n
	logrus.WithField("claim_limit", n).Info("worker: backfill claim limit set (from BACKFILL_CLAIM_LIMIT)")
}

// InitActivityLogger wires up the activity log logger so that RunMaintenanceHandler
// can call Purge to enforce the 7-day retention policy.
// Must be called once at startup, before MQ consumers are started.
func InitActivityLogger(l *activitylog.Logger) {
	actLogger = l
}
