package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/joho/godotenv"

	_ "github.com/sanjari-dev/geonera-ingestion/ent/runtime"
	"github.com/sanjari-dev/geonera-ingestion/internal/activitylog"
	"github.com/sanjari-dev/geonera-ingestion/internal/api"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/database"
	"github.com/sanjari-dev/geonera-ingestion/internal/dukascopy"
	"github.com/sanjari-dev/geonera-ingestion/internal/mq"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
	"github.com/sanjari-dev/geonera-ingestion/internal/runtimecollector"
	"github.com/sanjari-dev/geonera-ingestion/internal/worker"
)

func main() {
	// Load variables from .env. If the file is absent (production), OS environment is used.
	if err := godotenv.Load(); err != nil {
		log.Printf("warning: .env not loaded (%v), relying on OS environment", err)
	}

	ctx := context.Background()

	// ── Database ─────────────────────────────────────────────────────────────
	dbClient, sqlDB, err := database.NewEntClient(ctx)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer func() {
		if err := dbClient.Close(); err != nil {
			log.Printf("database close: %v", err)
		}
	}()

	appDAL := dal.New(dbClient, sqlDB)

	// ── Runtime metrics collector ─────────────────────────────────────────────
	runtimecollector.Start()

	// ── Cloudflare R2 client ──────────────────────────────────────────────────
	r2Client, err := r2.New(r2.Config{
		Endpoint:        os.Getenv("R2_ENDPOINT"),
		Bucket:          os.Getenv("R2_BUCKET"),
		AccessKeyID:     os.Getenv("R2_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("R2_SECRET_ACCESS_KEY"),
	})
	if err != nil {
		log.Fatalf("r2 client: %v", err)
	}

	// ── Dukascopy HTTP client ─────────────────────────────────────────────────
	dukClient := dukascopy.NewClient()

	// ── Wire external clients into worker package ─────────────────────────────
	worker.InitClients(r2Client, dukClient)

	// ── Database: run pending schema migrations ───────────────────────────────
	if err := database.RunMigrations(ctx); err != nil {
		log.Fatalf("database migrate: %v", err)
	}

	// ── Activity Logger ───────────────────────────────────────────────────────
	logDSN := os.Getenv("DATABASE_URL")
	logDB, err := sql.Open("postgres", logDSN)
	if err != nil {
		log.Fatalf("activity log db: %v", err)
	}
	defer func() { _ = logDB.Close() }()
	logDB.SetMaxIdleConns(3)
	logDB.SetConnMaxLifetime(30 * time.Minute)
	activityLogger := activitylog.New(logDB)
	activityLogger.CloseOrphans(ctx)
	worker.InitActivityLogger(activityLogger)

	// ── RabbitMQ ──────────────────────────────────────────────────────────────
	mqClient, err := mq.NewClient()
	if err != nil {
		log.Fatalf("rabbitmq: %v", err)
	}
	defer func() {
		if err := mqClient.Close(); err != nil {
			log.Printf("rabbitmq close: %v", err)
		}
	}()

	if err := mq.SetupConsumers(mqClient, appDAL, activityLogger); err != nil {
		log.Fatalf("mq consumers: %v", err)
	}

	// ── Fiber ─────────────────────────────────────────────────────────────────
	app := fiber.New()

	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,PATCH,DELETE,OPTIONS",
		AllowHeaders: "Origin,Content-Type,Accept,Authorization",
	}))

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("Hello World")
	})

	// Prometheus metrics — scraped by Prometheus at /metrics.
	// No auth required so the scraper does not need X-Ingestion-Secret.
	app.Get("/metrics", func(c *fiber.Ctx) error {
		c.Set(fiber.HeaderContentType, "text/plain; version=0.0.4; charset=utf-8")
		return c.SendString(runtimecollector.PrometheusText())
	})

	app.Static("/openapi.yaml", "./openapi.yaml")

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

	api.RegisterRoutes(app, appDAL, activityLogger, r2Client, os.Getenv("INGESTION_SECRET"))

	port := os.Getenv("APP_PORT")
	if port == "" {
		port = "3000"
	}

	log.Printf("server starting on :%s", port)
	if err := app.Listen(":" + port); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
