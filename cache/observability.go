package cache

import (
	"context"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationScope = "github.com/guilhermelinosp/hellnet-lib-cache/cache"
	modulePath           = "github.com/guilhermelinosp/hellnet-lib-cache"
)

type observability struct {
	inst       instrument.Instrumentation
	tracer     trace.Tracer
	meter      metric.Meter
	logger     instrument.Logger
	operations metric.Int64Counter
	duration   metric.Float64Histogram
}

func (o observability) observe(ctx context.Context, operation, result string, started time.Time) {
	instrument.Observe(ctx, o.operations, o.duration, started,
		attribute.String("operation", operation), attribute.String("result", result))
}

func newObservability(inst instrument.Instrumentation) observability {
	if inst == nil {
		inst = instrument.Noop()
	}
	s := instrument.NewScope(inst, instrumentationScope, modulePath)
	return observability{
		inst:       inst,
		tracer:     s.Tracer,
		meter:      s.Meter,
		logger:     s.Logger,
		operations: s.Int64Counter("hellnet.cache.operations"),
		duration:   s.Float64Histogram("hellnet.cache.operation.duration", metric.WithUnit("s")),
	}
}
