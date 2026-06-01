package api

import (
	"context"

	"github.com/gofiber/fiber/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/worker"
)

// RegisterRoutes mounts all /api/v1 endpoints on app.
// Every handler extracts the incoming OTel trace context from HTTP headers,
// fires the corresponding background worker goroutine, and returns 202 immediately.
func RegisterRoutes(app *fiber.App, d *dal.DAL) {
	v1 := app.Group("/api/v1")

	// ── Auto-Seeder & Pruning ─────────────────────────────────────────────────
	// Triggered every ~5 minutes from an external scheduler.
	v1.Post("/maintenance", func(c *fiber.Ctx) error {
		ctx := extractOtelCtx(c)
		go worker.RunMaintenanceHandler(ctx, d)
		return c.Status(fiber.StatusAccepted).JSON(triggered())
	})

	// ── Ticks Regular (T-0 / T-1 / T-2) ─────────────────────────────────────
	// Triggered every hour at minute :05.
	v1.Post("/ticks/regular", func(c *fiber.Ctx) error {
		ctx := extractOtelCtx(c)
		go worker.RunTickParentHandler(ctx, "REGULAR", d)
		return c.Status(fiber.StatusAccepted).JSON(triggered())
	})

	// ── Backfill Ticks ────────────────────────────────────────────────────────
	// Triggered every 10 minutes.
	v1.Post("/ticks/backfill", func(c *fiber.Ctx) error {
		ctx := extractOtelCtx(c)
		go worker.RunTickParentHandler(ctx, "BACKFILL", d)
		return c.Status(fiber.StatusAccepted).JSON(triggered())
	})

	// ── Candles Regular ───────────────────────────────────────────────────────
	// Daily seed at 00:00 UTC; aggregation run at 05:08 UTC.
	v1.Post("/candles/regular", func(c *fiber.Ctx) error {
		ctx := extractOtelCtx(c)
		go worker.RunCandleParentHandler(ctx, "REGULAR", d)
		return c.Status(fiber.StatusAccepted).JSON(triggered())
	})

	// ── Backfill Candles ──────────────────────────────────────────────────────
	// Triggered every 20 minutes (at :04, :24, :44).
	v1.Post("/candles/backfill", func(c *fiber.Ctx) error {
		ctx := extractOtelCtx(c)
		go worker.RunCandleParentHandler(ctx, "BACKFILL", d)
		return c.Status(fiber.StatusAccepted).JSON(triggered())
	})

	// ── Outbox Worker (SyncTask) ──────────────────────────────────────────────
	// Triggered periodically to drain pending SyncTask events.
	v1.Post("/sync", func(c *fiber.Ctx) error {
		ctx := extractOtelCtx(c)
		go worker.RunSyncHandler(ctx, d)
		return c.Status(fiber.StatusAccepted).JSON(triggered())
	})
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
	return otel.GetTextMapPropagator().Extract(
		c.UserContext(), // inherits the Fiber middleware server span
		propagation.HeaderCarrier(c.GetReqHeaders()),
	)
}

func triggered() fiber.Map {
	return fiber.Map{"status": "triggered_via_http"}
}
