// Package observability wires structured logging (slog), Prometheus metrics
// and OpenTelemetry tracing.
package observability

import (
	"context"
	"log/slog"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Metrics exposed on /metrics.
var (
	EventsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_events_processed_total",
		Help: "Payment events processed, by result (recorded, duplicate, invalid, dead_letter).",
	}, []string{"result"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency by route, method and status.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route", "method", "status"})

	ReconciliationDivergences = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "reconciliation_divergences_total",
		Help: "Divergences found between bank statement and ledger, by kind.",
	}, []string{"kind"})
)

// NewLogger returns a JSON logger at the given level ("debug", "info", ...).
func NewLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// SetupTracing configures an OTLP/HTTP exporter when OTEL_EXPORTER_OTLP_ENDPOINT
// is set (e.g. Jaeger or Grafana Tempo). It returns a shutdown function.
func SetupTracing(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(serviceName)))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
