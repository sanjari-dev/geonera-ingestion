package mq

import (
	"fmt"
	"log"
	"os"
	"sync"

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

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("mq: dial: %w", err)
	}

	return &Client{url: url, conn: conn}, nil
}

// Close closes the underlying AMQP connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Close()
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
		log.Println("mq: reconnected to RabbitMQ")
		c.conn = conn
	}

	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("mq: open channel: %w", err)
	}
	return ch, nil
}
