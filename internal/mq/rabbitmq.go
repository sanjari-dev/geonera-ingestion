package mq

import (
	"fmt"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

// NewRabbitMQConn dials RabbitMQ using RABBITMQ_URL from the environment.
// The caller is responsible for closing the connection via defer.
func NewRabbitMQConn() (*amqp.Connection, error) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		return nil, fmt.Errorf("RABBITMQ_URL is not set")
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	return conn, nil
}
