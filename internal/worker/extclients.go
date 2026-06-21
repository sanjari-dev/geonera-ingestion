package worker

import (
	"errors"
	"os"
	"strconv"

	"github.com/sirupsen/logrus"

	"github.com/sanjari-dev/geonera-ingestion/internal/activitylog"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
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
// HTTP client, starts the download rate limiter goroutine, and launches converter workers.
// Must be called once at startup, before the HTTP server and MQ consumers are started.
func InitClients(r2c *r2.Client, dc *dukascopy.Client, d *dal.DAL) {
	r2Client = r2c
	dukClient = dc
	initEnvConfig()
	InitDownloadRateLimiter()
	StartConverterWorkers(d)
}

// initEnvConfig reads configurations from the environment variables (e.g. BACKFILL_CLAIM_LIMIT, DUKASCOPY_MAX_RPS).
func initEnvConfig() {
	if v := os.Getenv("BACKFILL_CLAIM_LIMIT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			backfillMasterClaimLimit = n
			logrus.WithField("claim_limit", n).Info("worker: backfill claim limit set (from BACKFILL_CLAIM_LIMIT)")
		} else {
			logrus.WithFields(logrus.Fields{"value": v, "default": backfillMasterClaimLimit}).Warn("worker: invalid BACKFILL_CLAIM_LIMIT (must be a positive integer) — using default")
		}
	}

	if v := os.Getenv("DUKASCOPY_MAX_RPS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			dukascopyMaxRPS = n
			dukascopyBurst = n
			logrus.WithFields(logrus.Fields{"max_rps": n, "burst": n}).Info("worker: dukascopy max rps/burst set (from DUKASCOPY_MAX_RPS)")
		} else {
			logrus.WithFields(logrus.Fields{"value": v, "default": dukascopyMaxRPS}).Warn("worker: invalid DUKASCOPY_MAX_RPS (must be a positive integer) — using default")
		}
	}
}

// InitActivityLogger wires up the activity log logger so that RunMaintenanceHandler
// can call Purge to enforce the 7-day retention policy.
// Must be called once at startup, before MQ consumers are started.
func InitActivityLogger(l *activitylog.Logger) {
	actLogger = l
}
