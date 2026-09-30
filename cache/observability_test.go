package cache

import (
	"context"
	"testing"

	"github.com/guilhermelinosp/hellnet-lib-cache/internal/obstest"
	"go.opentelemetry.io/otel/attribute"
)

func TestContextFirstCacheSpanUsesCallerParent(t *testing.T) {
	harness := obstest.New(t)
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
}
