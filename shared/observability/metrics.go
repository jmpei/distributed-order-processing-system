package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// All collectors are auto-registered with prometheus.DefaultRegisterer at
// package init time. Each service's main.go just needs to mount
// promhttp.Handler() at /metrics.
var (
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests handled.",
		},
		[]string{"service", "method", "path", "status"},
	)

	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency in seconds.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"service", "method", "path"},
	)

	// SagaTotal counts saga transitions to a TERMINAL state (one of
	// COMPLETED / COMPENSATED / FAILED). Only emitted by order-service —
	// inventory + payment never own saga lifecycle.
	SagaTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "saga_total",
			Help: "Total sagas that reached a terminal state, labeled by status.",
		},
		[]string{"status"},
	)

	// SagaDuration is wall-clock seconds from saga creation to terminal
	// state. Observe at the same moment as SagaTotal.Inc.
	SagaDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "saga_duration_seconds",
			Help:    "End-to-end saga duration (creation to terminal state).",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 30, 60, 300, 600},
		},
		[]string{"status"},
	)

	// DLQMessages is the current message count in each dead-letter queue,
	// sampled periodically by each service's StartDLQSampler.
	DLQMessages = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dlq_messages",
			Help: "Current message count in each dead-letter queue.",
		},
		[]string{"queue"},
	)
)
