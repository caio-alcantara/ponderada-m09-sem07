package main

import (
	"testing"
	"time"
)

func TestRabbitMQURL(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "")
	if got := rabbitMQURL(); got != "amqp://guest:guest@rabbitmq:5672/" {
		t.Fatalf("unexpected default rabbitMQURL: %s", got)
	}

	t.Setenv("RABBITMQ_URL", "amqp://app:app123@rabbitmq:5672/")
	if got := rabbitMQURL(); got != "amqp://app:app123@rabbitmq:5672/" {
		t.Fatalf("unexpected env rabbitMQURL: %s", got)
	}
}

func TestQueueName(t *testing.T) {
	t.Setenv("RABBITMQ_QUEUE", "")
	if got := queueName(); got != "sensor_data_queue" {
		t.Fatalf("unexpected default queueName: %s", got)
	}

	t.Setenv("RABBITMQ_QUEUE", "custom_queue")
	if got := queueName(); got != "custom_queue" {
		t.Fatalf("unexpected env queueName: %s", got)
	}
}

func TestPostgresDSN(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "")
	if got := postgresDSN(); got == "" {
		t.Fatal("expected non-empty default postgresDSN")
	}

	expected := "host=localhost port=5432 user=test password=test dbname=test sslmode=disable"
	t.Setenv("POSTGRES_DSN", expected)
	if got := postgresDSN(); got != expected {
		t.Fatalf("unexpected env postgresDSN: %s", got)
	}
}

func TestBatchSize(t *testing.T) {
	t.Setenv("BATCH_SIZE", "")
	if got := batchSize(); got != 5000 {
		t.Fatalf("expected default batchSize 5000, got %d", got)
	}

	t.Setenv("BATCH_SIZE", "2500")
	if got := batchSize(); got != 2500 {
		t.Fatalf("expected batchSize 2500, got %d", got)
	}

	t.Setenv("BATCH_SIZE", "9999")
	if got := batchSize(); got != 5000 {
		t.Fatalf("expected capped batchSize 5000, got %d", got)
	}

	t.Setenv("BATCH_SIZE", "invalid")
	if got := batchSize(); got != 5000 {
		t.Fatalf("expected default batchSize for invalid input, got %d", got)
	}
}

func TestBatchFlushInterval(t *testing.T) {
	t.Setenv("BATCH_FLUSH_INTERVAL", "")
	if got := batchFlushInterval(); got != 2*time.Second {
		t.Fatalf("expected default flush interval 2s, got %s", got)
	}

	t.Setenv("BATCH_FLUSH_INTERVAL", "5s")
	if got := batchFlushInterval(); got != 5*time.Second {
		t.Fatalf("expected flush interval 5s, got %s", got)
	}

	t.Setenv("BATCH_FLUSH_INTERVAL", "invalid")
	if got := batchFlushInterval(); got != 2*time.Second {
		t.Fatalf("expected default flush interval for invalid input, got %s", got)
	}
}

func TestParseTimestamp(t *testing.T) {
	cases := []string{
		"2026-03-20T10:00:00Z",
		"2026-03-20 10:00:00",
		"2026-03-20T10:00:00",
	}

	for _, value := range cases {
		if _, err := parseTimestamp(value); err != nil {
			t.Fatalf("expected valid timestamp for %s, got error: %v", value, err)
		}
	}

	if _, err := parseTimestamp("20/03/2026 10:00:00"); err == nil {
		t.Fatal("expected error for invalid timestamp format")
	}
}
