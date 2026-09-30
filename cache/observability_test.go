package cache

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

func TestContextFirstCacheSpanUsesCallerParent(t *testing.T) {
	harness := telemetry.NewHarness(t)
	c, err := NewWithOptions(context.Background(), WithOptions(optsL1Only()), WithInstrumentation(harness))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, parent := harness.TracerProvider().Tracer("caller").Start(context.Background(), "caller")
	parentID := parent.SpanContext().SpanID()
	var out map[string]string
	if err := c.GetContext(ctx, "missing", &out); err != nil {
		t.Fatal(err)
	}
	parent.End()
	spans := harness.SpansByName("cache.get")
	if len(spans) != 1 {
		t.Fatalf("cache spans = %d, want 1", len(spans))
	}
	if spans[0].Parent().SpanID() != parentID {
		t.Fatal("cache span is not a child of caller span")
	}
	if got, ok := harness.CounterValue(context.Background(), "hellnet.cache.operations", attribute.String("operation", "get"), attribute.String("result", "miss")); !ok || got != 1 {
		t.Fatalf("cache operation counter = %d, %v", got, ok)
	}
	if got, ok := harness.HistogramCount(context.Background(), "hellnet.cache.operation.duration", attribute.String("operation", "get"), attribute.String("result", "miss")); !ok || got != 1 {
		t.Fatalf("cache operation duration count = %d, %v", got, ok)
	}
}

func TestHealthCheckEmitsErrorMetricsAndDuration(t *testing.T) {
	harness := telemetry.NewHarness(t)
	c, err := NewWithOptions(context.Background(), WithOptions(optsL1Only()), WithProviders(unhealthyProvider{name: "L2-External"}), WithInstrumentation(harness))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.HealthCheck(context.Background()); err == nil {
		t.Fatal("HealthCheck() error = nil, want unhealthy provider error")
	}

	attrs := []attribute.KeyValue{attribute.String("operation", "health"), attribute.String("result", "error")}
	if got, ok := harness.CounterValue(context.Background(), "hellnet.cache.operations", attrs...); !ok || got != 1 {
		t.Fatalf("health operation counter = %d, %v", got, ok)
	}
	if got, ok := harness.HistogramCount(context.Background(), "hellnet.cache.operation.duration", attrs...); !ok || got != 1 {
		t.Fatalf("health operation duration count = %d, %v", got, ok)
	}
	spans := harness.SpansByName("cache.health")
	if len(spans) != 1 || spans[0].Status().Code != codes.Error {
		t.Fatalf("health span status = %#v, want one error span", spans)
	}
}

func TestTelemetryNeverRecordsCacheKey(t *testing.T) {
	const secretKey = "customer:alice@example.com:session-token"
	harness := telemetry.NewHarness(t)
	failing := newStub("L2-External")
	failing.onSet = func(string, []byte, time.Duration) error { return errors.New("backend unavailable") }
	c, err := NewWithOptions(context.Background(), WithOptions(optsL1Only()), WithProviders(failing), WithInstrumentation(harness))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.SetContext(context.Background(), secretKey, "value", time.Minute); err == nil {
		t.Fatal("SetContext() error = nil, want provider failure")
	}

	attrs := []attribute.KeyValue{attribute.String("operation", "set"), attribute.String("result", "error")}
	if got, ok := harness.CounterValue(context.Background(), "hellnet.cache.operations", attrs...); !ok || got != 1 {
		t.Fatalf("set operation counter = %d, %v", got, ok)
	}
	if got, ok := harness.HistogramCount(context.Background(), "hellnet.cache.operation.duration", attrs...); !ok || got != 1 {
		t.Fatalf("set operation duration count = %d, %v", got, ok)
	}
	for _, span := range harness.SpansByName("cache.set") {
		for _, attr := range span.Attributes() {
			if strings.Contains(string(attr.Key), "key") || strings.Contains(attr.Value.AsString(), secretKey) {
				t.Fatalf("span contains sensitive cache key: %s=%s", attr.Key, attr.Value)
			}
		}
	}
	for _, record := range harness.Logs() {
		if strings.Contains(record.Body().AsString(), secretKey) {
			t.Fatal("log body contains sensitive cache key")
		}
		record.WalkAttributes(func(attr attribute.KeyValue) bool {
			if strings.Contains(string(attr.Key), "key") || strings.Contains(attr.Value.AsString(), secretKey) {
				t.Fatalf("log contains sensitive cache key: %s=%s", attr.Key, attr.Value)
			}
			return true
		})
	}
}

type unhealthyProvider struct{ name string }

func (p unhealthyProvider) Name() string                          { return p.name }
func (unhealthyProvider) Get(string) ([]byte, error)              { return nil, nil }
func (unhealthyProvider) Set(string, []byte, time.Duration) error { return nil }
func (unhealthyProvider) Remove(string) error                     { return nil }
func (unhealthyProvider) Exists(string) (bool, error)             { return false, nil }
func (unhealthyProvider) HealthCheck() bool                       { return false }
func (unhealthyProvider) Close() error                            { return nil }
