package cache

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationScope = "github.com/guilhermelinosp/hellnet-lib-cache/cache"

type observability struct {
	inst       instrument.Instrumentation
	tracer     trace.Tracer
	meter      metric.Meter
	logger     instrument.Logger
	operations metric.Int64Counter
	duration   metric.Float64Histogram
}

func (o observability) observe(ctx context.Context, operation, result string, started time.Time) {
	attrs := metric.WithAttributes(attribute.String("operation", operation), attribute.String("result", result))
	o.operations.Add(ctx, 1, attrs)
	o.duration.Record(ctx, time.Since(started).Seconds(), attrs)
}

func newObservability(inst instrument.Instrumentation) observability {
	if inst == nil {
		inst = instrument.Noop()
	}
	version := moduleVersion()
	logger := inst.Logger(instrumentationScope)
	meter := inst.MeterProvider().Meter(instrumentationScope, metric.WithInstrumentationVersion(version))
	operations, err := meter.Int64Counter("hellnet.cache.operations")
	if err != nil {
		logger.Error(context.TODO(), "cache metric creation failed", "metric", "hellnet.cache.operations", "error", err)
		meter = metricnoop.NewMeterProvider().Meter(instrumentationScope)
		operations, _ = meter.Int64Counter("hellnet.cache.operations")
	}
	duration, err := meter.Float64Histogram("hellnet.cache.operation.duration", metric.WithUnit("s"))
	if err != nil {
		logger.Error(context.TODO(), "cache metric creation failed", "metric", "hellnet.cache.operation.duration", "error", err)
		duration, _ = metricnoop.NewMeterProvider().Meter(instrumentationScope).Float64Histogram("hellnet.cache.operation.duration")
	}
	return observability{
		inst:       inst,
		tracer:     inst.TracerProvider().Tracer(instrumentationScope, trace.WithInstrumentationVersion(version)),
		meter:      meter,
		logger:     logger,
		operations: operations,
		duration:   duration,
	}
}

func moduleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/guilhermelinosp/hellnet-lib-cache" && dep.Version != "" {
			return dep.Version
		}
	}
	return "unknown"
}
