package mq

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/sanjari-dev/geonera-ingestion/internal/activitylog"
	"github.com/sanjari-dev/geonera-ingestion/internal/dal"
	"github.com/sanjari-dev/geonera-ingestion/internal/worker"
)

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
// e.g., "jobs.ticks.regular" → "ticks.regular", "jobs.maintenance" → "maintenance".
func jobNameFromQueue(queue string) string {
	return strings.TrimPrefix(queue, "jobs.")
}

// SetupConsumers declares all job queues and starts a self-healing goroutine
// consumer for each one.
//
// A dedicated channel is opened per queue so one slow consumer cannot starve
// the others. If a channel or connection dies, each consumer goroutine
// independently retries with exponential back-off (1 s → 32 s cap).
//
// After each successful handler run, queues marked publishEvent=true emit a
// "state_changed" signal on the events.ingestion fanout exchange so the
// admin-backend dashboard updates without constant DB polling.
func SetupConsumers(c *Client, d *dal.DAL, logger *activitylog.Logger) error {
	type subscription struct {
		queue        string
		handler      func(ctx context.Context, onStarted func()) bool
		publishEvent bool
	}

	subs := []subscription{
		{QueueTicksRegular, func(ctx context.Context, onStarted func()) bool {
			return worker.RunTickParentHandler(ctx, "REGULAR", d, onStarted)
		}, true},
		{QueueTicksBackfill, func(ctx context.Context, onStarted func()) bool {
			return worker.RunTickParentHandler(ctx, "BACKFILL", d, onStarted)
		}, true},
		{QueueCandlesRegular, func(ctx context.Context, onStarted func()) bool {
			return worker.RunCandleParentHandler(ctx, "REGULAR", d, onStarted)
		}, true},
		{QueueCandlesBackfill, func(ctx context.Context, onStarted func()) bool {
			return worker.RunCandleParentHandler(ctx, "BACKFILL", d, onStarted)
		}, true},
		{QueueMaintenance, func(ctx context.Context, onStarted func()) bool {
			return worker.RunMaintenanceHandler(ctx, d, onStarted)
		}, true},
		{QueueSync, func(ctx context.Context, onStarted func()) bool {
			return worker.RunSyncHandler(ctx, d, onStarted)
		}, false}, // sync touches ClickHouse, not the states table
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
		var onComplete func()
		if sub.publishEvent {
			onComplete = func() { c.PublishEvent("state_changed") }
		} else {
			onComplete = func() {}
		}
		go consumeLoop(c, sub.queue, sub.handler, logger, onComplete)
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
func consumeLoop(c *Client, queue string, handler func(ctx context.Context, onStarted func()) bool, logger *activitylog.Logger, onComplete func()) {
	backoff := time.Second
	for {
		err := runConsumer(c, queue, handler, logger, onComplete)
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
func runConsumer(c *Client, queue string, handler func(ctx context.Context, onStarted func()) bool, logger *activitylog.Logger, onComplete func()) error {
	ch, err := c.channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	if _, err = ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return err
	}

	// Limit to 1 unacknowledged message per consumer to bound the burst of
	// concurrent trigger goroutines per queue.
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

			ctx := context.Background()

			// Ack immediately so the broker can deliver the next message.
			_ = msg.Ack(false)

			go func(ctx context.Context) {
				jobName := jobNameFromQueue(queue)
				var logID uuid.UUID
				ran := handler(ctx, func() {
					logID = logger.Record(ctx, activitylog.SrcMQ, jobName, map[string]any{"queue": queue})
				})
				if ran {
					logger.Complete(logID)
					onComplete()
				}
			}(ctx)

		case amqpErr, ok := <-chClose:
			if !ok || amqpErr == nil {
				return nil
			}
			return amqpErr
		}
	}
}
