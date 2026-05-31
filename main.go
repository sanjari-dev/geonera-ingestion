package main

import (
	"context"
	"log"
	"os"

	"github.com/gofiber/contrib/otelfiber/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/joho/godotenv"
	"github.com/sanjari-dev/geonera-ingestion/internal/database"
	"github.com/sanjari-dev/geonera-ingestion/internal/mq"
	"github.com/sanjari-dev/geonera-ingestion/internal/seed"
	"github.com/sanjari-dev/geonera-ingestion/internal/telemetry"
)

func main() {
	// Load variables from .env. If the file is absent (production), OS environment is used.
	if err := godotenv.Load(); err != nil {
		log.Printf("warning: .env not loaded (%v), relying on OS environment", err)
	}

	ctx := context.Background()

	// ── OpenTelemetry ─────────────────────────────────────────────────────────
	shutdownTracer, err := telemetry.SetupTracer(ctx)
	if err != nil {
		log.Fatalf("telemetry: %v", err)
	}
	defer func() {
		if err := shutdownTracer(ctx); err != nil {
			log.Printf("telemetry shutdown: %v", err)
		}
	}()

	// ── Database (PostgreSQL via Ent) ─────────────────────────────────────────
	dbClient, err := database.NewEntClient(ctx)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer func() {
		if err := dbClient.Close(); err != nil {
			log.Printf("database close: %v", err)
		}
	}()

	// ── Seed: timeframes ──────────────────────────────────────────────────────
	// Inserts the 19 standard timeframes if they do not exist yet.
	if err := seed.Timeframes(ctx, dbClient); err != nil {
		log.Fatalf("seed timeframes: %v", err)
	}
	log.Print("timeframe seed complete")

	// ── RabbitMQ ──────────────────────────────────────────────────────────────
	mqConn, err := mq.NewRabbitMQConn()
	if err != nil {
		log.Fatalf("rabbitmq: %v", err)
	}
	defer func() {
		if err := mqConn.Close(); err != nil {
			log.Printf("rabbitmq close: %v", err)
		}
	}()

	// ── Fiber ─────────────────────────────────────────────────────────────────
	app := fiber.New()

	// CORS: allow Scalar (loaded from CDN) to fetch this API from the browser.
	// AllowOrigins "*" is acceptable for development; restrict to specific domains in production.
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,PATCH,DELETE,OPTIONS",
		AllowHeaders: "Origin,Content-Type,Accept,Authorization",
	}))

	// OTel middleware: automatically injects a trace span into every incoming HTTP request.
	app.Use(otelfiber.Middleware())

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hello World")
	})

	// Serve the OpenAPI specification as a static file.
	app.Static("/openapi.yaml", "./openapi.yaml")

	// Interactive Scalar API documentation page.
	app.Get("/docs", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTMLCharsetUTF8)
		return c.SendString(`<!doctype html>
<html>
  <head>
    <title>Geonera Ingestion API Docs</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body>
    <script
      id="api-reference"
      data-url="/openapi.yaml"
      src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  </body>
</html>`)
	})

	port := os.Getenv("APP_PORT")
	if port == "" {
		port = "3000"
	}

	log.Printf("server starting on :%s", port)
	log.Fatal(app.Listen(":" + port))
}
