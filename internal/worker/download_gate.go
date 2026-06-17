package worker

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/sanjari-dev/geonera-ingestion/ent"
	"github.com/sanjari-dev/geonera-ingestion/internal/dukascopy"
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
	select {
	case queue <- req:
		log.Printf("ticks/download: queued instrument=%s ts=%s queue=%s",
			instrName(row), row.Timestamp.Format("2006-01-02T15:04Z"), queueName)
	case <-ctx.Done():
		log.Printf("ticks/download: enqueue canceled instrument=%s ts=%s queue=%s err=%v",
			instrName(row), row.Timestamp.Format("2006-01-02T15:04Z"), queueName, ctx.Err())
		return DownloadResult{Status: DownloadError, Err: ctx.Err()}
	}

	// Wait for the worker to deliver a result. If ctx is canceled while
	// waiting, abandon the slot — the worker discards the result silently.
	select {
	case result := <-resultCh:
		return result
	case <-ctx.Done():
		log.Printf("ticks/download: wait canceled instrument=%s ts=%s err=%v",
			instrName(row), row.Timestamp.Format("2006-01-02T15:04Z"), ctx.Err())
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
	log.Printf("worker: %d download workers started (max %d req/s, backfill queue cap %d, regular queue prioritised over backfill)", dukascopyBurst, dukascopyMaxRPS, backfillMasterClaimLimit)
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

	// Discard already-canceled requests so they don't consume rate tokens.
	select {
	case <-req.ctx.Done():
		log.Printf("ticks/download: discard pre-rate-gate instrument=%s ts=%s err=%v", inst, ts, req.ctx.Err())
		req.resultCh <- DownloadResult{Status: DownloadError, Err: req.ctx.Err()}
		return
	default:
	}

	// Rate gate: wait for a token before sending the HTTP request.
	// Caps global throughput at dukascopyMaxRPS req/s.
	select {
	case <-tickDownloadRateGate:
		log.Printf("ticks/download: rate-gate acquired instrument=%s ts=%s", inst, ts)
	case <-req.ctx.Done():
		log.Printf("ticks/download: discard at rate-gate instrument=%s ts=%s err=%v", inst, ts, req.ctx.Err())
		req.resultCh <- DownloadResult{Status: DownloadError, Err: req.ctx.Err()}
		return
	}

	result := doDownload(req.ctx, req.row)

	// Deliver a result. If the caller canceled while we were downloading,
	// the sending on resultCh (buffered 1) succeeds, but nobody reads it — GC'd.
	select {
	case req.resultCh <- result:
	case <-req.ctx.Done():
		log.Printf("ticks/download: result discarded caller-canceled instrument=%s ts=%s", inst, ts)
	}
}

// doDownload performs the actual HTTP request and maps the raw error into a
// typed DownloadStatus so no caller ever needs to inspect error types directly.
func doDownload(ctx context.Context, row *ent.State) DownloadResult {
	inst := instrName(row)
	ts := row.Timestamp.Format("2006-01-02T15:04Z")
	log.Printf("ticks/download: fetching instrument=%s ts=%s", inst, ts)

	data, err := downloadBI5(ctx, row)
	if err == nil {
		log.Printf("ticks/download: OK instrument=%s ts=%s bytes=%d", inst, ts, len(data))
		return DownloadResult{Status: DownloadOK, Data: data}
	}
	switch {
	case isNotFoundError(err):
		log.Printf("ticks/download: NOT_FOUND HTTP 404 instrument=%s ts=%s", inst, ts)
		return DownloadResult{Status: DownloadNotFound, Err: err}
	case isRateLimitedError(err):
		log.Printf("ticks/download: RATE_LIMITED HTTP 429 instrument=%s ts=%s", inst, ts)
		return DownloadResult{Status: DownloadRateLimited, Err: err}
	default:
		var httpErr *dukascopy.HTTPError
		if errors.As(err, &httpErr) {
			log.Printf("ticks/download: ERROR %s instrument=%s ts=%s err=%v",
				httpClass(httpErr.StatusCode), inst, ts, err)
		} else {
			// Network error, timeout, or context cancellation — no HTTP status code.
			log.Printf("ticks/download: ERROR instrument=%s ts=%s err=%v", inst, ts, err)
		}
		return DownloadResult{Status: DownloadError, Err: err}
	}
}

// httpClass returns the HTTP status code class label for log output.
// e.g. 503 → "HTTP 503 (5xx)", 403 → "HTTP 403 (4xx)", 301 → "HTTP 301 (3xx)".
func httpClass(code int) string {
	return fmt.Sprintf("HTTP %d (%dxx)", code, code/100)
}
