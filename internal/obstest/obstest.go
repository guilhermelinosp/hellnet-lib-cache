// Package obstest contains the private in-memory observability harness used by
// this library's tests. It must never be imported by production packages.
package obstest

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/guilhermelinosp/hellnet-lib-telemetry/instrument"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

var _ instrument.Instrumentation = (*Harness)(nil)

type Harness struct {
	t    testing.TB
	tp   *sdktrace.TracerProvider
	mp   *sdkmetric.MeterProvider
	mr   *sdkmetric.ManualReader
	lp   *sdklog.LoggerProvider
	rec  *tracetest.SpanRecorder
	mu   sync.Mutex
	logs []sdklog.Record
}

type exporter struct{ h *Harness }

func (e *exporter) Export(_ context.Context, records []sdklog.Record) error {
	e.h.mu.Lock()
	defer e.h.mu.Unlock()
	for _, r := range records {
		e.h.logs = append(e.h.logs, r.Clone())
	}
	return nil
}
func (*exporter) Shutdown(context.Context) error   { return nil }
func (*exporter) ForceFlush(context.Context) error { return nil }

func New(t testing.TB) *Harness {
	t.Helper()
	h := &Harness{t: t, rec: tracetest.NewSpanRecorder(), mr: sdkmetric.NewManualReader()}
	h.tp = sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(h.rec))
	h.mp = sdkmetric.NewMeterProvider(sdkmetric.WithReader(h.mr))
	h.lp = sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(&exporter{h: h})))
	t.Cleanup(func() {
		_ = h.tp.Shutdown(context.Background())
		_ = h.mp.Shutdown(context.Background())
		_ = h.lp.Shutdown(context.Background())
	})
	return h
}
func (h *Harness) TracerProvider() trace.TracerProvider { return h.tp }
func (h *Harness) MeterProvider() metric.MeterProvider  { return h.mp }
func (h *Harness) Propagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
}
func (h *Harness) Logger(scope string) instrument.Logger { return &logger{inner: h.lp.Logger(scope)} }
func (h *Harness) Spans() []sdktrace.ReadOnlySpan        { return h.rec.Ended() }
func (h *Harness) SpansByName(name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range h.Spans() {
		if s.Name() == name {
			out = append(out, s)
		}
	}
	return out
}
func ChildOf(parent, child sdktrace.ReadOnlySpan) bool {
	return child.Parent().SpanID() == parent.SpanContext().SpanID()
}
func (h *Harness) Logs() []sdklog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]sdklog.Record, len(h.logs))
	for i := range h.logs {
		out[i] = h.logs[i].Clone()
	}
	return out
}
func (h *Harness) LogsBySeverity(severity log.Severity) []sdklog.Record {
	var out []sdklog.Record
	for _, r := range h.Logs() {
		if r.Severity() == severity {
			out = append(out, r)
		}
	}
	return out
}

func (h *Harness) metrics(ctx context.Context) metricdata.ResourceMetrics {
	var out metricdata.ResourceMetrics
	if err := h.mr.Collect(ctx, &out); err != nil {
		h.t.Fatalf("collect metrics: %v", err)
	}
	return out
}
func (h *Harness) metric(name string, ctx context.Context, attrs ...attribute.KeyValue) (metricdata.Metrics, bool) {
	want := attribute.NewSet(attrs...)
	data := h.metrics(ctx)
	for _, sm := range data.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return metricdata.Metrics{Name: m.Name, Description: m.Description, Unit: m.Unit, Data: matchingData(m.Data, &want)}, true
			}
		}
	}
	return metricdata.Metrics{}, false
}
func matchingData(data metricdata.Aggregation, want *attribute.Set) metricdata.Aggregation {
	switch d := data.(type) {
	case metricdata.Sum[int64]:
		var p []metricdata.DataPoint[int64]
		for _, x := range d.DataPoints {
			if x.Attributes.Equals(want) {
				p = append(p, x)
			}
		}
		d.DataPoints = p
		return d
	case metricdata.Gauge[int64]:
		var p []metricdata.DataPoint[int64]
		for _, x := range d.DataPoints {
			if x.Attributes.Equals(want) {
				p = append(p, x)
			}
		}
		d.DataPoints = p
		return d
	case metricdata.Gauge[float64]:
		var p []metricdata.DataPoint[float64]
		for _, x := range d.DataPoints {
			if x.Attributes.Equals(want) {
				p = append(p, x)
			}
		}
		d.DataPoints = p
		return d
	case metricdata.Histogram[int64]:
		var p []metricdata.HistogramDataPoint[int64]
		for _, x := range d.DataPoints {
			if x.Attributes.Equals(want) {
				p = append(p, x)
			}
		}
		d.DataPoints = p
		return d
	}
	return data
}
func (h *Harness) CounterValue(ctx context.Context, name string, attrs ...attribute.KeyValue) (int64, bool) {
	m, ok := h.metric(name, ctx, attrs...)
	if !ok {
		return 0, false
	}
	d, ok := m.Data.(metricdata.Sum[int64])
	if !ok || len(d.DataPoints) != 1 {
		return 0, false
	}
	return d.DataPoints[0].Value, true
}
func (h *Harness) HistogramCount(ctx context.Context, name string, attrs ...attribute.KeyValue) (uint64, bool) {
	m, ok := h.metric(name, ctx, attrs...)
	if !ok {
		return 0, false
	}
	d, ok := m.Data.(metricdata.Histogram[int64])
	if !ok || len(d.DataPoints) != 1 {
		return 0, false
	}
	return d.DataPoints[0].Count, true
}
func (h *Harness) GaugeValue(ctx context.Context, name string, attrs ...attribute.KeyValue) (float64, bool) {
	m, ok := h.metric(name, ctx, attrs...)
	if !ok {
		return 0, false
	}
	if d, ok := m.Data.(metricdata.Gauge[float64]); ok && len(d.DataPoints) == 1 {
		return d.DataPoints[0].Value, true
	}
	if d, ok := m.Data.(metricdata.Gauge[int64]); ok && len(d.DataPoints) == 1 {
		return float64(d.DataPoints[0].Value), true
	}
	return 0, false
}

type logger struct{ inner log.Logger }

func (l *logger) emit(ctx context.Context, sev log.Severity, msg string, args ...any) {
	if ctx == nil {
		ctx = context.Background()
	}
	r := log.Record{}
	r.SetSeverity(sev)
	r.SetSeverityText(sev.String())
	r.SetBody(attribute.StringValue(msg))
	for i := 0; i < len(args); i += 2 {
		key := "arg"
		if k, ok := args[i].(string); ok {
			key = k
		}
		var value any
		if i+1 < len(args) {
			value = args[i+1]
		}
		r.AddAttributes(attribute.String(key, fmt.Sprint(value)))
	}
	l.inner.Emit(ctx, r)
}
func (l *logger) Debug(ctx context.Context, msg string, args ...any) {
	l.emit(ctx, log.SeverityDebug, msg, args...)
}
func (l *logger) Info(ctx context.Context, msg string, args ...any) {
	l.emit(ctx, log.SeverityInfo, msg, args...)
}
func (l *logger) Warn(ctx context.Context, msg string, args ...any) {
	l.emit(ctx, log.SeverityWarn, msg, args...)
}
func (l *logger) Error(ctx context.Context, msg string, args ...any) {
	l.emit(ctx, log.SeverityError, msg, args...)
}
