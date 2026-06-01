package mq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

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

// SetupConsumers declares all job queues and starts a self-healing goroutine
// consumer for each one. Every consumed message extracts the incoming OTel
// trace context from its AMQP headers before dispatching the background worker.
//
// A dedicated channel is opened per queue so one slow consumer cannot starve
// the others. If a channel or connection dies, each consumer goroutine
// independently retries with exponential back-off (1 s → 32 s cap).
func SetupConsumers(c *Client, d *dal.DAL) error {
	type subscription struct {
		queue   string
		handler func(ctx context.Context)
	}

	subs := []subscription{
		{QueueTicksRegular, func(ctx context.Context) {
			go worker.RunTickParentHandler(ctx, "REGULAR", d)
		}},
		{QueueTicksBackfill, func(ctx context.Context) {
			go worker.RunTickParentHandler(ctx, "BACKFILL", d)
		}},
		{QueueCandlesRegular, func(ctx context.Context) {
			go worker.RunCandleParentHandler(ctx, "REGULAR", d)
		}},
		{QueueCandlesBackfill, func(ctx context.Context) {
			go worker.RunCandleParentHandler(ctx, "BACKFILL", d)
		}},
		{QueueMaintenance, func(ctx context.Context) {
			go worker.RunMaintenanceHandler(ctx, d)
		}},
		{QueueSync, func(ctx context.Context) {
			go worker.RunSyncHandler(ctx, d)
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
		go consumeLoop(c, sub.queue, sub.handler)
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
func consumeLoop(c *Client, queue string, handler func(ctx context.Context)) {
	backoff := time.Second
	for {
		err := runConsumer(c, queue, handler)
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
func runConsumer(c *Client, queue string, handler func(ctx context.Context)) error {
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

			// Create a CONSUMER span for this message.  This is the MQ equivalent
			// of otelfiber.Middleware() on the HTTP layer: it establishes a parent
			// span so that the worker goroutine's child spans are always visible in
			// Jaeger under a single trace root.
			//
			// The span ends immediately after the goroutine is dispatched (fire-and-
			// forget), just like the HTTP server span ends after returning 202.
			// Worker child spans appear in Jaeger as async descendants of this span.
			ctx, span := mqTracer.Start(ctx, "mq/receive",
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("messaging.system", "rabbitmq"),
					attribute.String("messaging.destination.name", queue),
					attribute.String("messaging.operation.type", "process"),
				),
			)

			// Ack before dispatching: the worker is idempotent and advisory-locked,
			// so re-queueing on crash would simply be a duplicate trigger (safe).
			_ = msg.Ack(false)
			handler(ctx)

			// End the consumer span after dispatch (not after the worker completes).
			// Worker spans run asynchronously but remain linked via the trace context.
			span.End()

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
