package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEnvInt(t *testing.T) {
	t.Setenv("TEST_INT", "42")
	if got := envInt("TEST_INT", 10); got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}

	t.Setenv("TEST_INT", "invalid")
	if got := envInt("TEST_INT", 10); got != 10 {
		t.Fatalf("expected default 10 for invalid value, got %d", got)
	}

	t.Setenv("TEST_INT", "0")
	if got := envInt("TEST_INT", 10); got != 10 {
		t.Fatalf("expected default 10 for non-positive value, got %d", got)
	}
}

func TestWorkerCountAndPublishBufferSize(t *testing.T) {
	t.Setenv("PUBLISH_WORKERS", "16")
	t.Setenv("PUBLISH_BUFFER", "30000")

	if got := workerCount(); got != 16 {
		t.Fatalf("expected workerCount 16, got %d", got)
	}

	if got := publishBufferSize(); got != 30000 {
		t.Fatalf("expected publishBufferSize 30000, got %d", got)
	}
}

func TestPublisherEnqueue(t *testing.T) {
	p := &Publisher{jobs: make(chan []byte, 1)}

	if ok := p.enqueue([]byte("payload")); !ok {
		t.Fatal("expected enqueue to succeed with available buffer")
	}

	if got := len(p.jobs); got != 1 {
		t.Fatalf("expected queue length 1, got %d", got)
	}

	if ok := p.enqueue([]byte("another")); ok {
		t.Fatal("expected enqueue to fail when buffer is full")
	}
}

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()

	healthHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}
	if body := rr.Body.String(); body != "ok" {
		t.Fatalf("expected body 'ok', got %q", body)
	}
}

func TestHealthHandlerMethodNotAllowed(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	rr := httptest.NewRecorder()

	healthHandler(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", rr.Code)
	}
}

func TestSensorDataHandler(t *testing.T) {
	originalPublisher := publisher
	defer func() { publisher = originalPublisher }()

	t.Run("method not allowed", func(t *testing.T) {
		publisher = &Publisher{jobs: make(chan []byte, 1)}
		req := httptest.NewRequest(http.MethodGet, "/sensor_data", nil)
		rr := httptest.NewRecorder()

		sensorDataHandler(rr, req)

		if rr.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected status 405, got %d", rr.Code)
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		publisher = &Publisher{jobs: make(chan []byte, 1)}
		req := httptest.NewRequest(http.MethodPost, "/sensor_data", strings.NewReader("{invalid"))
		rr := httptest.NewRecorder()

		sensorDataHandler(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected status 400, got %d", rr.Code)
		}
	})

	t.Run("queue overloaded", func(t *testing.T) {
		publisher = &Publisher{jobs: make(chan []byte)}
		payload := `{"id_dispositivo":1,"timestamp":"2026-03-20T10:00:00Z","tipo_sensor":"temperatura","tipo_leitura":"analogica","valor":23.5}`
		req := httptest.NewRequest(http.MethodPost, "/sensor_data", strings.NewReader(payload))
		rr := httptest.NewRecorder()

		sensorDataHandler(rr, req)

		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected status 503, got %d", rr.Code)
		}
	})

	t.Run("success", func(t *testing.T) {
		publisher = &Publisher{jobs: make(chan []byte, 1)}
		payload := `{"id_dispositivo":1,"timestamp":"2026-03-20T10:00:00Z","tipo_sensor":"temperatura","tipo_leitura":"analogica","valor":23.5}`
		req := httptest.NewRequest(http.MethodPost, "/sensor_data", strings.NewReader(payload))
		rr := httptest.NewRecorder()

		sensorDataHandler(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status 200, got %d", rr.Code)
		}
		if body := rr.Body.String(); body != "received" {
			t.Fatalf("expected body 'received', got %q", body)
		}
		if got := len(publisher.jobs); got != 1 {
			t.Fatalf("expected 1 queued message, got %d", got)
		}
	})
}
