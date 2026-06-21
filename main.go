package main

import (
	"context"
	"database/sql"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"

	_ "github.com/sanjari-dev/geonera-ingestion/ent/runtime"
	"github.com/sanjari-dev/geonera-ingestion/internal/activitylog"
	"github.com/sanjari-dev/geonera-ingestion/internal/api"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/database"
	"github.com/sanjari-dev/geonera-ingestion/internal/dukascopy"
	_ "github.com/sanjari-dev/geonera-ingestion/internal/logger"
	"github.com/sanjari-dev/geonera-ingestion/internal/mq"
	"github.com/sanjari-dev/geonera-ingestion/internal/r2"
	"github.com/sanjari-dev/geonera-ingestion/internal/runtimecollector"
	"github.com/sanjari-dev/geonera-ingestion/internal/worker"
)

func main() {
	// Load variables from .env. If the file is absent (production), OS environment is used.
	if err := godotenv.Load(); err != nil {
		logrus.WithError(err).Warn("warning: .env not loaded, relying on OS environment")
	}

	ctx := context.Background()

	// ── Database ─────────────────────────────────────────────────────────────
	dbClient, sqlDB, err := database.NewEntClient(ctx)
	if err != nil {
		logrus.WithError(err).Fatal("database: init failed")
	}
	defer func() {
		if err := dbClient.Close(); err != nil {
			logrus.WithError(err).Error("database close")
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
		logrus.WithError(err).Fatal("r2 client: init failed")
	}

	// ── Dukascopy HTTP client ─────────────────────────────────────────────────
	dukClient := dukascopy.NewClient()

	// ── Wire external clients into worker package ─────────────────────────────
	worker.InitClients(r2Client, dukClient, appDAL)

	// ── Database: run pending schema migrations ───────────────────────────────
	if err := database.RunMigrations(ctx); err != nil {
		logrus.WithError(err).Fatal("database migrate: failed")
	}

	// ── Activity Logger ───────────────────────────────────────────────────────
	logDSN := os.Getenv("DATABASE_URL")
	logDB, err := sql.Open("postgres", logDSN)
	if err != nil {
		logrus.WithError(err).Fatal("activity log db: init failed")
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
		logrus.WithError(err).Fatal("rabbitmq: init failed")
	}
	defer func() {
		if err := mqClient.Close(); err != nil {
			logrus.WithError(err).Error("rabbitmq close")
		}
	}()

	if err := mq.SetupConsumers(mqClient, appDAL, activityLogger); err != nil {
		logrus.WithError(err).Fatal("mq consumers: setup failed")
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
	// No auth required, so the scraper does not need X-Ingestion-Secret.
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

	logrus.WithField("port", port).Info("server starting")
	if err := app.Listen(":" + port); err != nil {
		logrus.WithError(err).Fatal("server error")
	}
}
