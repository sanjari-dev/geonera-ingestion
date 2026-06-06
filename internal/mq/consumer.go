package mq

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/sanjari-dev/geonera-ingestion/internal/activitylog"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/worker"
)

// mqTracer is the OTel tracer for the MQ consumer layer.
// It creates a consumer span for each incoming message, acting as the
// MQ equivalent of otelfiber.Middleware() on the HTTP layer.
var mqTracer = otel.Tracer("mq/consumer")

// Queue names — must match the names used by the external scheduler.
const (
	QueueTicksRegular    = "jobs.ticks.regular"
	QueueTicksBackfill   = "jobs.ticks.backfill"
	QueueCandlesRegular  = "jobs.candles.regular"
	QueueCandlesBackfill = "jobs.candles.backfill"
	QueueMaintenance     = "jobs.maintenance"
	QueueSync            = "jobs.sync"
)

// jobNameFromQueue strips the "jobs." prefix from a queue name so that MQ
// and HTTP triggers share the same job_name in the activity log.
// e.g. "jobs.ticks.regular" → "ticks.regular", "jobs.maintenance" → "maintenance".
func jobNameFromQueue(queue string) string {
	return strings.TrimPrefix(queue, "jobs.")
}

// SetupConsumers declares all job queues and starts a self-healing goroutine
// consumer for each one. Every consumed message extracts the incoming OTel
// trace context from its AMQP headers before dispatching the background worker.
//
// A dedicated channel is opened per queue so one slow consumer cannot starve
// the others. If a channel or connection dies, each consumer goroutine
// independently retries with exponential back-off (1 s → 32 s cap).
func SetupConsumers(c *Client, d *dal.DAL, logger *activitylog.Logger) error {
	type subscription struct {
		queue   string
		handler func(ctx context.Context, onStarted func()) bool
	}

	subs := []subscription{
		{QueueTicksRegular, func(ctx context.Context, onStarted func()) bool {
			return worker.RunTickParentHandler(ctx, "REGULAR", d, onStarted)
		}},
		{QueueTicksBackfill, func(ctx context.Context, onStarted func()) bool {
			return worker.RunTickParentHandler(ctx, "BACKFILL", d, onStarted)
		}},
		{QueueCandlesRegular, func(ctx context.Context, onStarted func()) bool {
			return worker.RunCandleParentHandler(ctx, "REGULAR", d, onStarted)
		}},
		{QueueCandlesBackfill, func(ctx context.Context, onStarted func()) bool {
			return worker.RunCandleParentHandler(ctx, "BACKFILL", d, onStarted)
		}},
		{QueueMaintenance, func(ctx context.Context, onStarted func()) bool {
			return worker.RunMaintenanceHandler(ctx, d, onStarted)
		}},
		{QueueSync, func(ctx context.Context, onStarted func()) bool {
			return worker.RunSyncHandler(ctx, d, onStarted)
		}},
	}

	// Perform one synchronous probe per queue at startup, so any misconfiguration
	// (wrong queue name, broker permissions) surfaces immediately as a fatal error
	// rather than being silently swallowed inside a goroutine.
	for _, sub := range subs {
		if err := probeQueue(c, sub.queue); err != nil {
			return err
		}
	}

	// Start a persistent, self-healing consumer goroutine for each queue.
	for _, sub := range subs {
		go consumeLoop(c, sub.queue, sub.handler, logger)
	}

	return nil
}

// probeQueue declares the queue once to validate connectivity and permissions.
// It opens and immediately closes a throw-away channel.
func probeQueue(c *Client, queue string) error {
	ch, err := c.channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	return err
}

// consumeLoop runs forever, restarting the inner consumption session whenever the
// channel drops (network blip, broker restart, channel-level error).
// Back-off starts at 1 s and doubles up to 32 s to avoid thundering herds.
func consumeLoop(c *Client, queue string, handler func(ctx context.Context, onStarted func()) bool, logger *activitylog.Logger) {
	backoff := time.Second
	for {
		err := runConsumer(c, queue, handler, logger)
		if err != nil {
			log.Printf("mq: consumer %q error: %v — retrying in %s", queue, err, backoff)
		} else {
			// Channel closed cleanly (broker-side shutdown); reset back-off.
			log.Printf("mq: consumer %q channel closed — reconnecting", queue)
			backoff = time.Second
		}
		time.Sleep(backoff)
		if backoff < 32*time.Second {
			backoff *= 2
		}
	}
}

// runConsumer opens one channel, declares the queue, sets QoS, and drains
// messages until the channel closes. It returns nil on a clean channel close
// and an error on any setup or protocol failure.
func runConsumer(c *Client, queue string, handler func(ctx context.Context, onStarted func()) bool, logger *activitylog.Logger) error {
	ch, err := c.channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	if _, err = ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return err
	}

	// Limit to 1 unacknowledged message per consumer so a burst of triggers
	// does not queue up work that the advisory lock would discard anyway.
	if err = ch.Qos(1, 0, false); err != nil {
		return err
	}

	msgs, err := ch.Consume(
		queue,
		"",    // server-generated consumer tag
		false, // manual ack — we ack before dispatching so a crash re-queues the message
		false, // non-exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return err
	}

	// Watch for channel-level errors (e.g., broker-forced close) alongside messages.
	chClose := ch.NotifyClose(make(chan *amqp.Error, 1))

	log.Printf("mq: consumer %q ready", queue)
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				// msgs channel closed; let consumeLoop retry.
				return nil
			}
			// Extract incoming trace context from AMQP headers (W3C traceparent).
			// If no traceparent is present, ctx carries no span and the consumer
			// span below becomes the root of a new trace.
			ctx := extractMQOtelCtx(msg.Headers)

			// Create a CONSUMER span for this message. This establishes a parent
			// span so that the worker's child spans are always visible in
			// Jaeger under a single trace root.
			//
			// Because the handler is synchronous, this root span accurately
			// measures the total end-to-end processing time of the job.
			ctx, span := mqTracer.Start(ctx, "mq/receive",
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("messaging.system", "rabbitmq"),
					attribute.String("messaging.destination.name", queue),
					attribute.String("messaging.operation.type", "process"),
				),
			)

			// Ack immediately so the broker can deliver the next message right
			// away. This prevents stale triggers from piling up in RabbitMQ
			// while a long-running job holds the advisory lock.
			_ = msg.Ack(false)

			// Dispatch in a goroutine so the consumer loop is free to receive and
			// drop the next message. logger.Record is called inside onStarted —
			// only AFTER the advisory lock is successfully acquired — so dropped
			// triggers (lock busy) never appear in the activity dashboard at all.
			go func(ctx context.Context, sp trace.Span) {
				defer sp.End()
				jobName := jobNameFromQueue(queue)
				var logID uuid.UUID
				ran := handler(ctx, func() {
					logID = logger.Record(ctx, activitylog.SrcMQ, jobName, map[string]any{"queue": queue})
				})
				if ran {
					logger.Complete(logID)
				}
			}(ctx, span)

		case amqpErr, ok := <-chClose:
			if !ok || amqpErr == nil {
				return nil
			}
			return amqpErr
		}
	}
}

// extractMQOtelCtx converts AMQP headers (map[string]any) to the string map
// the OTel text-map propagator expects, then extracts the trace context.
func extractMQOtelCtx(headers amqp.Table) context.Context {
	carrier := make(propagation.MapCarrier, len(headers))
	for k, v := range headers {
		if s, ok := v.(string); ok {
			carrier[k] = s
		}
	}
	return otel.GetTextMapPropagator().Extract(context.Background(), carrier)
}
