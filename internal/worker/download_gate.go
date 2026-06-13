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

// downloadQueue is the bounded channel through which calling goroutines submit
// BI5 download requests to the fixed worker pool. The buffer allows up to
// dukascopyBurst requests to be queued before callers block, matching the
// worker count so the queue drains as fast as it fills under a full load.
var downloadQueue = make(chan downloadRequest, dukascopyBurst)

// RequestDownload submits a BI5 download request to the worker pool and blocks
// until a result arrives or ctx is canceled. It is the single entry point for
// all BI5 downloads — ETL functions never call downloadBI5 directly.
//
// Concurrency contract: exactly dukascopyBurst goroutines execute HTTP requests
// at any time (one per worker). The rate gate inside each worker additionally
// caps global throughput at dukascopyMaxRPS req/s.
func RequestDownload(ctx context.Context, row *ent.State) DownloadResult {
	resultCh := make(chan DownloadResult, 1)

	// Enqueue the request. If ctx is canceled before a worker picks it up,
	// return immediately without consuming a rate-gate token.
	select {
	case downloadQueue <- downloadRequest{ctx: ctx, row: row, resultCh: resultCh}:
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
// downloadQueue. Called once by InitDownloadRateLimiter at startup.
func startDownloadWorkers() {
	for i := 0; i < dukascopyBurst; i++ {
		go runDownloadWorker()
	}
	log.Printf("worker: %d download workers started (max %d req/s)", dukascopyBurst, dukascopyMaxRPS)
}

// runDownloadWorker is the body of each download worker goroutine. It drains
// downloadQueue indefinitely, applying the rate gate before each HTTP request.
func runDownloadWorker() {
	for req := range downloadQueue {
		// Discard already-canceled requests so they don't consume rate tokens.
		select {
		case <-req.ctx.Done():
			req.resultCh <- DownloadResult{Status: DownloadError, Err: req.ctx.Err()}
			continue
		default:
		}

		// Rate gate: wait for a token before sending the HTTP request.
		// Caps global throughput at dukascopyMaxRPS req/s.
		select {
		case <-tickDownloadRateGate:
		case <-req.ctx.Done():
			req.resultCh <- DownloadResult{Status: DownloadError, Err: req.ctx.Err()}
			continue
		}

		result := doDownload(req.ctx, req.row)

		// Deliver a result. If the caller canceled while we were downloading,
		// to send on resultCh (buffered 1) succeeds, but nobody reads it — GC'd.
		select {
		case req.resultCh <- result:
		case <-req.ctx.Done():
		}
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
