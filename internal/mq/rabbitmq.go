package mq

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Client wraps an AMQP connection and transparently re-dials on failure.
// All channel operations go through Client.channel() which re-establishes
// the underlying TCP connection when it has been closed.
type Client struct {
	url  string
	mu   sync.Mutex
	conn *amqp.Connection
}

// NewClient dials RabbitMQ using RABBITMQ_URL from the environment.
// The caller is responsible for calling Close when the application exits.
func NewClient() (*Client, error) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		return nil, fmt.Errorf("RABBITMQ_URL is not set")
	}

	var conn *amqp.Connection
	var err error

	// Retry up to 15 times (approx 30 seconds) to allow RabbitMQ to fully boot.
	// RabbitMQ's health check might pass before its AMQP listeners are ready.
	for i := 0; i < 15; i++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			break
		}
		logrus.WithError(err).Error("mq: dial failed, retrying in 2s")
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("mq: dial timeout: %w", err)
	}

	return &Client{url: url, conn: conn}, nil
}

// Close closes the underlying AMQP connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Close()
}

// stateChangedExchange is the fanout exchange used to signal state changes to
// admin-backend consumers. Messages are transient (signal-only); only the
// exchange itself is durable so it survives a broker restart.
const stateChangedExchange = "events.ingestion"

// PublishEvent publishes a fire-and-forget signal on the fanout exchange
// "events.ingestion". The body carries the event type, but consumers treat
// any arriving message as a "dirty" signal and re-query the database
// themselves — so the body is informational only.
//
// Errors are logged and swallowed. Missed publishing delays a dashboard
// refresh by at most one 60-second fallback cycle on the subscriber side.
func (c *Client) PublishEvent(eventType string) {
	ch, err := c.channel()
	if err != nil {
		logrus.WithError(err).Error("mq: PublishEvent: open channel")
		return
	}
	defer func(ch *amqp.Channel) {
		_ = ch.Close()
	}(ch)

	if err := ch.ExchangeDeclare(
		stateChangedExchange,
		"fanout",
		true,  // durable — survives broker restart
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		logrus.WithError(err).Error("mq: PublishEvent: declare exchange")
		return
	}

	body := []byte(`{"event":"` + eventType + `"}`)
	if err := ch.Publish(
		stateChangedExchange,
		"",    // routing key (ignored for fanout)
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Transient,
			Body:         body,
		},
	); err != nil {
		logrus.WithError(err).Error("mq: PublishEvent: publish")
	}
}

// Channel opens a new AMQP channel. If the connection has been closed, it
// re-dials first, so callers never need to manage reconnection themselves.
func (c *Client) channel() (*amqp.Channel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn.IsClosed() {
		conn, err := amqp.Dial(c.url)
		if err != nil {
			return nil, fmt.Errorf("mq: reconnect: %w", err)
		}
		logrus.Info("mq: reconnected to RabbitMQ")
		c.conn = conn
	}

	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("mq: open channel: %w", err)
	}
	return ch, nil
}
