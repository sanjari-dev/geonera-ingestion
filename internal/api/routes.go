package api

import (
	"context"

	"github.com/google/uuid"
	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/sanjari-dev/geonera-ingestion/internal/activitylog"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
	"github.com/sanjari-dev/geonera-ingestion/internal/runtimecollector"
	"github.com/sanjari-dev/geonera-ingestion/internal/worker"
)

// RegisterRoutes mounts all /api/v1 endpoints on app.
// Every handler extracts the incoming OTel trace context from HTTP headers,
// fires the corresponding background worker goroutine, and returns 202 immediately.
func RegisterRoutes(app *fiber.App, d *dal.DAL, logger *activitylog.Logger, r2c *r2.Client) {
	v1 := app.Group("/api/v1")

	// ── Auto-Seeder & Pruning ─────────────────────────────────────────────────
	// Triggered every ~5 minutes from an external scheduler.
	// httpTrigger fires a worker in the background, recording activity only
	// after the advisory lock is acquired. Dropped triggers (lock busy) produce
	// no activity log entry — they are silently discarded.
	httpTrigger := func(ctx context.Context, meta map[string]any, jobName string,
		run func(onStarted func()) bool,
	) {
		go func() {
			var id uuid.UUID
			ran := run(func() {
				id = logger.Record(ctx, activitylog.SrcHTTP, jobName, meta)
			})
			if ran {
				logger.Complete(id)
			}
		}()
	}

	v1.Post("/maintenance", func(c *fiber.Ctx) error {
		ctx, meta := extractOtelCtx(c), httpMeta(c)
		httpTrigger(ctx, meta, "maintenance", func(onStarted func()) bool {
			return worker.RunMaintenanceHandler(ctx, d, onStarted)
		})
		return c.Status(fiber.StatusAccepted).JSON(triggered())
	})

	// ── Ticks Regular (T-0 / T-1 / T-2) ─────────────────────────────────────
	v1.Post("/ticks/regular", func(c *fiber.Ctx) error {
		ctx, meta := extractOtelCtx(c), httpMeta(c)
		httpTrigger(ctx, meta, "ticks.regular", func(onStarted func()) bool {
			return worker.RunTickParentHandler(ctx, "REGULAR", d, onStarted)
		})
		return c.Status(fiber.StatusAccepted).JSON(triggered())
	})

	// ── Backfill Ticks ────────────────────────────────────────────────────────
	v1.Post("/ticks/backfill", func(c *fiber.Ctx) error {
		ctx, meta := extractOtelCtx(c), httpMeta(c)
		httpTrigger(ctx, meta, "ticks.backfill", func(onStarted func()) bool {
			return worker.RunTickParentHandler(ctx, "BACKFILL", d, onStarted)
		})
		return c.Status(fiber.StatusAccepted).JSON(triggered())
	})

	// ── Candles Regular ───────────────────────────────────────────────────────
	v1.Post("/candles/regular", func(c *fiber.Ctx) error {
		ctx, meta := extractOtelCtx(c), httpMeta(c)
		httpTrigger(ctx, meta, "candles.regular", func(onStarted func()) bool {
			return worker.RunCandleParentHandler(ctx, "REGULAR", d, onStarted)
		})
		return c.Status(fiber.StatusAccepted).JSON(triggered())
	})

	// ── Backfill Candles ──────────────────────────────────────────────────────
	v1.Post("/candles/backfill", func(c *fiber.Ctx) error {
		ctx, meta := extractOtelCtx(c), httpMeta(c)
		httpTrigger(ctx, meta, "candles.backfill", func(onStarted func()) bool {
			return worker.RunCandleParentHandler(ctx, "BACKFILL", d, onStarted)
		})
		return c.Status(fiber.StatusAccepted).JSON(triggered())
	})

	// ── Outbox Worker (SyncTask) ──────────────────────────────────────────────
	v1.Post("/sync", func(c *fiber.Ctx) error {
		ctx, meta := extractOtelCtx(c), httpMeta(c)
		httpTrigger(ctx, meta, "sync", func(onStarted func()) bool {
			return worker.RunSyncHandler(ctx, d, onStarted)
		})
		return c.Status(fiber.StatusAccepted).JSON(triggered())
	})

	// ── Object Storage Stats ──────────────────────────────────────────────────
	// Returns file count and total bytes per instrument and job type.
	// Results are cached for 5 minutes — first call may be slower on large buckets.
	v1.Get("/storage", func(c *fiber.Ctx) error {
		stats, err := r2c.StorageStats(c.Context())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(stats)
	})

	// ── Process Runtime Metrics ───────────────────────────────────────────────
	// Returns CPU%, heap memory, goroutine count, and uptime for this process.
	// Metrics are sampled every 5 seconds by the background collector.
	// A cpu_percent of -1 means the first sample interval has not elapsed yet.
	v1.Get("/runtime", func(c *fiber.Ctx) error {
		return c.JSON(runtimecollector.Get())
	})

	// ── Activity Logs ─────────────────────────────────────────────────────────
	// Returns a paginated list of all job trigger events (MQ + HTTP).
	// Query params: limit (default 50, max 200), offset (default 0),
	//               job_name (optional filter), trigger_src (optional: MQ|HTTP).
	v1.Get("/activity-logs", func(c *fiber.Ctx) error {
		limit := clampInt(c.QueryInt("limit", 50), 1, 200)
		offset := max0(c.QueryInt("offset", 0))
		jobName := c.Query("job_name")
		src := c.Query("trigger_src")

		result, err := logger.List(c.Context(), jobName, src, limit, offset)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(result)
	})
}

// httpMeta builds the meta map stored in the activity log for HTTP triggers.
func httpMeta(c *fiber.Ctx) map[string]any {
	return map[string]any{
		"method":     c.Method(),
		"path":       c.Path(),
		"ip":         c.IP(),
		"user_agent": c.Get(fiber.HeaderUserAgent),
	}
}

// clampInt constrains v to [lo, hi].
func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// max0 returns v if v >= 0, else 0.
func max0(v int) int {
	if v < 0 {
		return 0
	}
	return v
}

// extractOtelCtx returns a context that carries the OTel trace information for
// the incoming Fiber request, suitable for passing to background worker goroutines.
//
// Design rationale — why c.UserContext() instead of context.Background():
//
// otelfiber.Middleware() creates an HTTP server span and stores it in
// c.UserContext(). If we pass context.Background() to Extract(), we get a
// fresh context that has no knowledge of that Fiber span, so any goroutine
// launched from the handler creates a brand-new root trace completely
// disconnected from the HTTP request span in Jaeger.
//
// By passing c.UserContext() as the base:
//   - When the caller sends a W3C traceparent header: Extract() injects the
//     remote span context on top of the Fiber span, linking the worker to the
//     upstream distributed trace.
//   - When the caller sends no traceparent header: c.UserContext() already
//     contains the Fiber-created root span, so the worker goroutine's spans
//     automatically appear as children of the HTTP request span in Jaeger.
//
// Result: in both cases, the full trace tree is:
//
//	POST /api/v1/ticks/regular (HTTP server span)
//	  └─ RunTickParentHandler_REGULAR
//	        ├─ ticks/T0
//	        │    └─ ticks/etl → ticks/download-bi5, ticks/upload-r2 …
//	        ├─ ticks/T1
//	        └─ ticks/T2
func extractOtelCtx(c *fiber.Ctx) context.Context {
	ctx := otel.GetTextMapPropagator().Extract(
		c.UserContext(), // inherits the Fiber middleware server span
		propagation.HeaderCarrier(c.GetReqHeaders()),
	)
	
	// CRITICAL: Fiber's UserContext is tied to the HTTP request lifecycle.
	// Since we return 202 Accepted immediately, Fiber will cancel this context,
	// which would instantly kill the background worker spawned by the handler.
	// We use context.WithoutCancel to decouple the cancellation signal while
	// retaining the OTel trace context for distributed tracing.
	return context.WithoutCancel(ctx)
}

func triggered() fiber.Map {
	return fiber.Map{"status": "triggered_via_http"}
}
