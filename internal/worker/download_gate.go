package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/internal/dukascopy"
	ilogger "github.com/sanjari-dev/geonera-ingestion/internal/logger"
)

// DownloadStatus is the typed outcome of a Dukascopy BI5 download request.
// Callers switch on this value to decide the next state transition without
// inspecting raw error types.
type DownloadStatus int8

const (
	DownloadOK          DownloadStatus = iota // HTTP 2xx — Data contains BI5 bytes
	DownloadNotFound                          // HTTP 404 — slot does not exist at a source
	DownloadRateLimited                       // HTTP 429 — Dukascopy rate limit exceeded
	DownloadError                             // Any other failure (5xx, timeout, network)
)

// DownloadResult is what RequestDownload returns to the calling goroutine.
type DownloadResult struct {
	Status DownloadStatus
	Data   []byte // non-nil only when Status == DownloadOK
	Err    error  // nil when Status == DownloadOK
}

type downloadRequest struct {
	ctx      context.Context
	row      *ent.State
	resultCh chan<- DownloadResult
}

// regularDownloadQueue carries requests from T-0 / T-1 / T-2 Regular.
// Workers always drain this queue before touching backfillDownloadQueue.
var regularDownloadQueue = make(chan downloadRequest, dukascopyBurst)

// backfillDownloadQueue carries requests from the Backfill master-claim batch.
// Initialized in startDownloadWorkers (after env is read) so its capacity always
// matches backfillMasterClaimLimit, ensuring a full batch never blocks on submission.
var backfillDownloadQueue chan downloadRequest

// RequestDownload submits a BI5 download request to the worker pool and blocks
// until a result arrives or ctx is canceled. It is the single entry point for
// all BI5 downloads — ETL functions never call downloadBI5 directly.
//
// highPriority=true routes the request to regularDownloadQueue; workers always
// drain that queue before backfillDownloadQueue, so Regular tick phases are
// never starved by a concurrent Backfill batch.
//
// Concurrency contract: exactly dukascopyBurst goroutines execute HTTP requests
// at any time (one per worker). The rate gate inside each worker additionally
// caps global throughput at dukascopyMaxRPS req/s.
func RequestDownload(ctx context.Context, row *ent.State, highPriority bool) DownloadResult {
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "RequestDownload", "instrument": instrName(row), "ts": row.Timestamp.Format("2006-01-02T15:04Z"), "high_priority": highPriority}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "RequestDownload").Trace("fn_exit")

	resultCh := make(chan DownloadResult, 1)
	req := downloadRequest{ctx: ctx, row: row, resultCh: resultCh}

	queue := backfillDownloadQueue
	queueName := "backfill"
	if highPriority {
		queue = regularDownloadQueue
		queueName = "regular"
	}

	// Enqueue the request. If ctx is canceled before a worker picks it up,
	// return immediately without consuming a rate-gate token.
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "queue": queueName}).Trace("before channel: enqueue")
	select {
	case queue <- req:
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "queue": queueName}).Trace("after channel: enqueued")
		logrus.WithFields(logrus.Fields{"instrument": instrName(row), "ts": row.Timestamp.Format("2006-01-02T15:04Z"), "queue": queueName}).Info("ticks/download: queued")
	case <-ctx.Done():
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "queue": queueName}).Trace("branch: enqueue_ctx_canceled")
		logrus.WithFields(logrus.Fields{"instrument": instrName(row), "ts": row.Timestamp.Format("2006-01-02T15:04Z"), "queue": queueName}).WithError(ctx.Err()).Warn("ticks/download: enqueue canceled")
		return DownloadResult{Status: DownloadError, Err: ctx.Err()}
	}

	// Wait for the worker to deliver a result. If ctx is canceled while
	// waiting, abandon the slot — the worker discards the result silently.
	ilogger.T(ctx).WithField("instrument", instrName(row)).Trace("before channel: wait result")
	select {
	case result := <-resultCh:
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": instrName(row), "status": result.Status}).Trace("after channel: result received")
		return result
	case <-ctx.Done():
		ilogger.T(ctx).WithField("instrument", instrName(row)).Trace("branch: wait_ctx_canceled")
		logrus.WithFields(logrus.Fields{"instrument": instrName(row), "ts": row.Timestamp.Format("2006-01-02T15:04Z")}).WithError(ctx.Err()).Warn("ticks/download: wait canceled")
		return DownloadResult{Status: DownloadError, Err: ctx.Err()}
	}
}

// startDownloadWorkers initialises backfillDownloadQueue with the (env-resolved)
// backfillMasterClaimLimit capacity, then launches dukascopyBurst long-lived
// goroutines that drain both download queues with Regular priority.
// Called once by InitDownloadRateLimiter at startup, after initBackfillConfig.
func startDownloadWorkers() {
	backfillDownloadQueue = make(chan downloadRequest, backfillMasterClaimLimit)
	for i := 0; i < dukascopyBurst; i++ {
		go runDownloadWorker()
	}
	logrus.WithFields(logrus.Fields{"workers": dukascopyBurst, "max_rps": dukascopyMaxRPS, "backfill_queue_cap": backfillMasterClaimLimit}).Info("worker: download workers started (regular queue prioritised over backfill)")
}

// runDownloadWorker is the body of each download worker goroutine.
// It applies a biased elect: regularDownloadQueue is always checked first
// (non-blocking). Only when the regular queue is empty does the worker block
// on either queue — ensuring Regular tick phases are never delayed by a
// concurrent Backfill batch occupying all worker slots.
func runDownloadWorker() {
	for {
		req := pickDownloadRequest()
		processDownloadRequest(req)
	}
}

// pickDownloadRequest implements the two-level biased select:
//  1. Non-blocking check of regularDownloadQueue — if a Regular request is
//     waiting, take it immediately regardless of the backfill queue state.
//  2. Blocking select on both queues — reached only when the regular queue is
//     empty; serves whichever arrives first (fair between the two queues in
//     that state).
func pickDownloadRequest() downloadRequest {
	select {
	case req := <-regularDownloadQueue:
		return req
	default:
	}
	select {
	case req := <-regularDownloadQueue:
		return req
	case req := <-backfillDownloadQueue:
		return req
	}
}

// processDownloadRequest runs the rate-gate check and the actual HTTP download
// for one request, then delivers the result to the caller's resultCh.
func processDownloadRequest(req downloadRequest) {
	inst := instrName(req.row)
	ts := req.row.Timestamp.Format("2006-01-02T15:04Z")
	ilogger.T(req.ctx).WithFields(logrus.Fields{"fn": "processDownloadRequest", "instrument": inst, "ts": ts}).Trace("fn_entry")
	defer ilogger.T(req.ctx).WithField("fn", "processDownloadRequest").Trace("fn_exit")

	// Discard already-canceled requests so they don't consume rate tokens.
	ilogger.T(req.ctx).WithFields(logrus.Fields{"instrument": inst}).Trace("before check: pre-rate-gate ctx")
	select {
	case <-req.ctx.Done():
		ilogger.T(req.ctx).WithFields(logrus.Fields{"instrument": inst}).Trace("branch: discard_pre_rate_gate")
		logrus.WithFields(logrus.Fields{"instrument": inst, "ts": ts}).WithError(req.ctx.Err()).Warn("ticks/download: discard pre-rate-gate")
		req.resultCh <- DownloadResult{Status: DownloadError, Err: req.ctx.Err()}
		return
	default:
	}

	// Rate gate: wait for a token before sending the HTTP request.
	// Caps global throughput at dukascopyMaxRPS req/s.
	ilogger.T(req.ctx).WithFields(logrus.Fields{"instrument": inst}).Trace("before channel: rate-gate wait")
	select {
	case <-tickDownloadRateGate:
		ilogger.T(req.ctx).WithFields(logrus.Fields{"instrument": inst}).Trace("after channel: rate-gate acquired")
		logrus.WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Info("ticks/download: rate-gate acquired")
	case <-req.ctx.Done():
		ilogger.T(req.ctx).WithFields(logrus.Fields{"instrument": inst}).Trace("branch: discard_at_rate_gate")
		logrus.WithFields(logrus.Fields{"instrument": inst, "ts": ts}).WithError(req.ctx.Err()).Warn("ticks/download: discard at rate-gate")
		req.resultCh <- DownloadResult{Status: DownloadError, Err: req.ctx.Err()}
		return
	}

	ilogger.T(req.ctx).WithFields(logrus.Fields{"instrument": inst}).Trace("before call: doDownload")
	result := doDownload(req.ctx, req.row)
	ilogger.T(req.ctx).WithFields(logrus.Fields{"instrument": inst, "status": result.Status}).Trace("after call: doDownload")

	// Deliver a result. If the caller canceled while we were downloading,
	// the sending on resultCh (buffered 1) succeeds, but nobody reads it — GC'd.
	ilogger.T(req.ctx).WithFields(logrus.Fields{"instrument": inst}).Trace("before channel: deliver result")
	select {
	case req.resultCh <- result:
		ilogger.T(req.ctx).WithFields(logrus.Fields{"instrument": inst}).Trace("after channel: result delivered")
	case <-req.ctx.Done():
		ilogger.T(req.ctx).WithFields(logrus.Fields{"instrument": inst}).Trace("branch: result_discarded_caller_canceled")
		logrus.WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Warn("ticks/download: result discarded caller-canceled")
	}
}

// doDownload performs the actual HTTP request and maps the raw error into a
// typed DownloadStatus so no caller ever needs to inspect error types directly.
func doDownload(ctx context.Context, row *ent.State) DownloadResult {
	inst := instrName(row)
	ts := row.Timestamp.Format("2006-01-02T15:04Z")
	ilogger.T(ctx).WithFields(logrus.Fields{"fn": "doDownload", "instrument": inst, "ts": ts}).Trace("fn_entry")
	defer ilogger.T(ctx).WithField("fn", "doDownload").Trace("fn_exit")

	start := time.Now()
	logrus.WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Info("ticks/download: fetching")

	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts}).Trace("before call: downloadBI5")
	data, err := downloadBI5(ctx, row)
	ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "err": err != nil}).Trace("after call: downloadBI5")

	if err == nil {
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst, "bytes": len(data)}).Trace("branch: download_ok")
		logrus.WithFields(logrus.Fields{"instrument": inst, "ts": ts, "bytes": len(data), "elapsed": time.Since(start).Round(time.Millisecond)}).Info("ticks/download: OK")
		return DownloadResult{Status: DownloadOK, Data: data}
	}
	switch {
	case isNotFoundError(err):
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst}).Trace("branch: not_found")
		logrus.WithFields(logrus.Fields{"instrument": inst, "ts": ts, "elapsed": time.Since(start).Round(time.Millisecond)}).Info("ticks/download: NOT_FOUND HTTP 404")
		return DownloadResult{Status: DownloadNotFound, Err: err}
	case isRateLimitedError(err):
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst}).Trace("branch: rate_limited")
		logrus.WithFields(logrus.Fields{"instrument": inst, "ts": ts, "elapsed": time.Since(start).Round(time.Millisecond)}).Warn("ticks/download: RATE_LIMITED HTTP 429")
		return DownloadResult{Status: DownloadRateLimited, Err: err}
	default:
		ilogger.T(ctx).WithFields(logrus.Fields{"instrument": inst}).Trace("branch: download_error")
		var httpErr *dukascopy.HTTPError
		if errors.As(err, &httpErr) {
			logrus.WithError(err).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "elapsed": time.Since(start).Round(time.Millisecond), "url": httpClass(httpErr.StatusCode)}).Error("ticks/download: ERROR")
		} else {
			// Network error, timeout, or context cancellation — no HTTP status code.
			logrus.WithError(err).WithFields(logrus.Fields{"instrument": inst, "ts": ts, "elapsed": time.Since(start).Round(time.Millisecond)}).Error("ticks/download: ERROR")
		}
		return DownloadResult{Status: DownloadError, Err: err}
	}
}

// httpClass returns the HTTP status code class label for log output.
// e.g. 503 → "HTTP 503 (5xx)", 403 → "HTTP 403 (4xx)", 301 → "HTTP 301 (3xx)".
func httpClass(code int) string {
	return fmt.Sprintf("HTTP %d (%dxx)", code, code/100)
}
