package worker

import (
	"context"
	"log"

	"github.com/sanjari-dev/geonera-ingestion/ent"
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
// Sized to backfillMasterClaimLimit so a full batch never blocks on submission.
var backfillDownloadQueue = make(chan downloadRequest, backfillMasterClaimLimit)

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
	if highPriority {
		queue = regularDownloadQueue
	}

	// Enqueue the request. If ctx is canceled before a worker picks it up,
	// return immediately without consuming a rate-gate token.
	select {
	case queue <- req:
	case <-ctx.Done():
		return DownloadResult{Status: DownloadError, Err: ctx.Err()}
	}

	// Wait for the worker to deliver a result. If ctx is canceled while
	// waiting, abandon the slot — the worker discards the result silently.
	select {
	case result := <-resultCh:
		return result
	case <-ctx.Done():
		return DownloadResult{Status: DownloadError, Err: ctx.Err()}
	}
}

// startDownloadWorkers launches dukascopyBurst long-lived goroutines that drain
// both download queues with Regular priority. Called once by InitDownloadRateLimiter at startup.
func startDownloadWorkers() {
	for i := 0; i < dukascopyBurst; i++ {
		go runDownloadWorker()
	}
	log.Printf("worker: %d download workers started (max %d req/s, regular queue prioritised over backfill)",
		dukascopyBurst, dukascopyMaxRPS)
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
	// Discard already-canceled requests so they don't consume rate tokens.
	select {
	case <-req.ctx.Done():
		req.resultCh <- DownloadResult{Status: DownloadError, Err: req.ctx.Err()}
		return
	default:
	}

	// Rate gate: wait for a token before sending the HTTP request.
	// Caps global throughput at dukascopyMaxRPS req/s.
	select {
	case <-tickDownloadRateGate:
	case <-req.ctx.Done():
		req.resultCh <- DownloadResult{Status: DownloadError, Err: req.ctx.Err()}
		return
	}

	result := doDownload(req.ctx, req.row)

	// Deliver a result. If the caller canceled while we were downloading,
	// the sending on resultCh (buffered 1) succeeds, but nobody reads it — GC'd.
	select {
	case req.resultCh <- result:
	case <-req.ctx.Done():
	}
}

// doDownload performs the actual HTTP request and maps the raw error into a
// typed DownloadStatus so no caller ever needs to inspect error types directly.
func doDownload(ctx context.Context, row *ent.State) DownloadResult {
	data, err := downloadBI5(ctx, row)
	if err == nil {
		return DownloadResult{Status: DownloadOK, Data: data}
	}
	switch {
	case isNotFoundError(err):
		return DownloadResult{Status: DownloadNotFound, Err: err}
	case isRateLimitedError(err):
		return DownloadResult{Status: DownloadRateLimited, Err: err}
	default:
		return DownloadResult{Status: DownloadError, Err: err}
	}
}
