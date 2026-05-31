package telemetry

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// SetupTracer initialises the global TracerProvider with an OTLP HTTP exporter.
// It reads OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_SERVICE_NAME from the environment.
// The returned shutdown function must be deferred in the main to flush pending spans.
func SetupTracer(ctx context.Context) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return nil, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT is not set")
	}

	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = "geonera-ingestion"
	}

	// OTLP HTTP exporter (protocol: http/protobuf).
	// WithEndpointURL accepts a full URL including a scheme (http:// or https://).
	// The exporter automatically appends /v1/traces to the provided endpoint.
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
	}

	// Resource describes this service in Jaeger / Grafana Tempo.
	// Merged with resource.Default() to include SDK and runtime metadata.
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	// Register as the global provider so any package can obtain a tracer via otel.Tracer().
	otel.SetTracerProvider(tp)

	// W3C TraceContext + Baggage propagation for distributed tracing across services.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}
