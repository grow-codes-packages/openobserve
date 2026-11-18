package openobserve

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Default trace query windows (coordinator decision 4).
const (
	// DefaultTraceSearchWindow is searched by SearchTraces without a start.
	DefaultTraceSearchWindow = time.Hour
	// DefaultGetTraceWindow is searched by GetTrace.
	DefaultGetTraceWindow = 7 * 24 * time.Hour
	// DefaultServicesWindow is searched by ListServices and ListOperations.
	DefaultServicesWindow = 24 * time.Hour
)

// DefaultOTLPGRPCPort is the OTLP/gRPC port of OpenObserve.
const DefaultOTLPGRPCPort = "5081"

// Traces implements TraceQueryClient for one OpenObserve traces stream.
type Traces struct {
	c      *Client
	stream string
}

var _ TraceQueryClient = (*Traces)(nil)

// Traces returns the TraceQueryClient for Config.TracesStream.
func (c *Client) Traces() *Traces { return c.TracesFor("") }

// TracesFor returns the TraceQueryClient for stream ("" = Config.TracesStream).
func (c *Client) TracesFor(stream string) *Traces {
	if stream == "" {
		stream = c.cfg.TracesStream
	}
	return &Traces{c: c, stream: normalizeStreamName(stream)}
}

// Stream returns the (normalized) traces stream.
func (t *Traces) Stream() string { return t.stream }

// GetTrace returns the trace with all its spans (GET
// /api/{org}/{stream}/traces/{id}/details over the last 7 days, following
// pages) or nil when it does not exist.
func (t *Traces) GetTrace(ctx context.Context, traceID string) (*Trace, error) {
	const op = "traces.getTrace"
	id, err := NormalizeTraceID(traceID)
	if err != nil {
		return nil, err
	}
	end := time.Now().Add(time.Minute)
	start := end.Add(-DefaultGetTraceWindow)
	q := url.Values{"start_time": {micros(start)}, "end_time": {micros(end)}}
	var spans []Span
	for page := 0; page < 1000; page++ {
		var body struct {
			Hits      []map[string]any `json:"hits"`
			HasMore   bool             `json:"has_more"`
			NextAfter *struct {
				StartTime json.Number `json:"start_time"`
				SpanID    string      `json:"span_id"`
			} `json:"next_after"`
		}
		err := t.c.getJSON(ctx, op, &request{Method: http.MethodGet, Path: t.c.orgPath("/" + t.stream + "/traces/" + id + "/details"), Query: q}, &body)
		if err != nil {
			return nil, err
		}
		for _, row := range body.Hits {
			spans = append(spans, spanFromRow(row))
		}
		if !body.HasMore || body.NextAfter == nil || body.NextAfter.StartTime == "" {
			break
		}
		q.Set("after_start_time", body.NextAfter.StartTime.String())
		q.Set("after_span_id", body.NextAfter.SpanID)
	}
	if len(spans) == 0 {
		return nil, nil
	}
	return &Trace{TraceID: id, Spans: SortSpans(spans)}, nil
}

// internalSpanColumns are stored columns that never become span attributes.
var internalSpanColumns = map[string]bool{
	"_timestamp": true, "_o2_ingest_ts": true, "flags": true, "events": true, "links": true,
	"trace_id": true, "span_id": true, "reference_parent_span_id": true, "reference_parent_trace_id": true,
	"reference_ref_type": true, "operation_name": true, "span_kind": true, "service_name": true,
	"start_time": true, "end_time": true, "duration": true, "span_status": true, "status_code": true,
	"status_message": true,
}

var spanKinds = map[string]SpanKind{
	"0": SpanKindUnspecified, "1": SpanKindInternal, "2": SpanKindServer,
	"3": SpanKindClient, "4": SpanKindProducer, "5": SpanKindConsumer,
}

// spanFromRow maps a stored span row (getTrace.details) to a Span.
func spanFromRow(row map[string]any) Span {
	nsToMicros := func(v any) int64 { n, _ := toInt64(v); return n / 1000 }
	s := Span{
		TraceID:      strings.ToLower(toString(row["trace_id"])),
		SpanID:       strings.ToLower(toString(row["span_id"])),
		ParentSpanID: strings.ToLower(toString(row["reference_parent_span_id"])),
		Name:         toString(row["operation_name"]),
		Kind:         SpanKindUnspecified,
		ServiceName:  toString(row["service_name"]),
		StartMicros:  nsToMicros(row["start_time"]),
		EndMicros:    nsToMicros(row["end_time"]),
		Status:       SpanStatus{Code: SpanStatusUnset, Message: toString(row["status_message"])},
		Attributes:   map[string]any{},
		Events:       []SpanEvent{},
		Links:        []SpanLink{},
	}
	if k, ok := spanKinds[toString(row["span_kind"])]; ok {
		s.Kind = k
	}
	s.DurationMicros = max(s.EndMicros-s.StartMicros, 0)
	switch strings.ToUpper(toString(row["span_status"])) {
	case "OK":
		s.Status.Code = SpanStatusOK
	case "ERROR":
		s.Status.Code = SpanStatusError
	}
	for k, v := range row {
		if internalSpanColumns[k] || strings.HasPrefix(k, "infer_service_") || strings.HasPrefix(k, "o2_db_") {
			continue
		}
		if n := NormalizeAttrValue(v); n != nil {
			s.Attributes[k] = n
		}
	}
	var events []map[string]any
	if decodeJSONString(row["events"], &events) {
		for _, e := range events {
			ev := SpanEvent{Name: toString(e["name"]), TimeMicros: nsToMicros(e["_timestamp"]), Attributes: map[string]any{}}
			for k, v := range e {
				if k != "name" && k != "_timestamp" {
					ev.Attributes[k] = NormalizeAttrValue(v)
				}
			}
			s.Events = append(s.Events, ev)
		}
	}
	var links []map[string]any
	if decodeJSONString(row["links"], &links) {
		for _, l := range links {
			ctxMap, _ := l["context"].(map[string]any)
			link := SpanLink{TraceID: strings.ToLower(toString(ctxMap["traceId"])), SpanID: strings.ToLower(toString(ctxMap["spanId"])), Attributes: map[string]any{}}
			for k, v := range l {
				if k != "context" && k != "droppedAttributesCount" {
					link.Attributes[k] = NormalizeAttrValue(v)
				}
			}
			s.Links = append(s.Links, link)
		}
	}
	return s
}

// decodeJSONString decodes a column that holds JSON text (numbers as json.Number).
func decodeJSONString(v any, out any) bool {
	s, ok := v.(string)
	if !ok || s == "" {
		return false
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	return dec.Decode(out) == nil
}

// traceFilter builds the traces/latest filter (an SQL WHERE fragment).
func traceFilter(q TraceSearchQuery) string {
	var parts []string
	if q.Service != "" {
		parts = append(parts, "service_name = "+sqlString(q.Service))
	}
	if q.Operation != "" {
		parts = append(parts, "operation_name = "+sqlString(q.Operation))
	}
	if q.MinDuration > 0 {
		parts = append(parts, "duration >= "+strconv.FormatInt(q.MinDuration.Microseconds(), 10))
	}
	if q.MaxDuration > 0 {
		parts = append(parts, "duration <= "+strconv.FormatInt(q.MaxDuration.Microseconds(), 10))
	}
	keys := make([]string, 0, len(q.Tags))
	for k := range q.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if col := normalizeKey(k); col != "" {
			parts = append(parts, sqlIdent(col)+" = "+sqlString(q.Tags[k]))
		}
	}
	return strings.Join(parts, " AND ")
}

// SearchTraces finds traces (GET /api/{org}/{stream}/traces/latest), newest
// first, over the last hour by default. Tag keys are flattened like the stored
// columns (http.method → http_method). With filters, each summary covers only
// the matching spans (start, duration and span count), as OpenObserve reports.
func (t *Traces) SearchTraces(ctx context.Context, query TraceSearchQuery) ([]TraceSummary, error) {
	const op = "traces.searchTraces"
	query = query.WithDefaults()
	start, end, err := timeWindow(op, query.Start, query.End, DefaultTraceSearchWindow)
	if err != nil {
		return nil, err
	}
	q := url.Values{"start_time": {micros(start)}, "end_time": {micros(end)}, "from": {"0"}, "size": {strconv.Itoa(query.Limit)}}
	if f := traceFilter(query); f != "" {
		q.Set("filter", f)
	}
	var body struct {
		Hits []struct {
			TraceID    string        `json:"trace_id"`
			StartTime  json.Number   `json:"start_time"`
			Duration   json.Number   `json:"duration"`
			Spans      []json.Number `json:"spans"`
			FirstEvent struct {
				ServiceName   string `json:"service_name"`
				OperationName string `json:"operation_name"`
			} `json:"first_event"`
		} `json:"hits"`
	}
	if err := t.c.getJSON(ctx, op, &request{Method: http.MethodGet, Path: t.c.orgPath("/" + t.stream + "/traces/latest"), Query: q}, &body); err != nil {
		return nil, err
	}
	out := make([]TraceSummary, 0, len(body.Hits))
	for _, h := range body.Hits {
		startNs, _ := toInt64(h.StartTime)
		dur, _ := toInt64(h.Duration)
		ts := TraceSummary{
			TraceID: strings.ToLower(h.TraceID), RootServiceName: h.FirstEvent.ServiceName, RootName: h.FirstEvent.OperationName,
			StartMicros: startNs / 1000, DurationMicros: dur,
		}
		if len(h.Spans) > 0 {
			n, _ := toInt64(h.Spans[0])
			ts.SpanCount = int(n)
		}
		out = append(out, ts)
	}
	return out, nil
}

// distinct runs a SELECT DISTINCT over the traces stream (last 24 hours).
func (t *Traces) distinct(ctx context.Context, op, column, where string) ([]string, error) {
	sql := "SELECT DISTINCT " + column + " FROM " + sqlIdent(t.stream)
	if where != "" {
		sql += " WHERE " + where
	}
	sql += " ORDER BY " + column
	end := time.Now().Add(time.Minute)
	w, err := t.c.searchRaw(ctx, op, sql, end.Add(-DefaultServicesWindow), end, 0, 10000, false, StreamTypeTraces)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(w.Hits))
	for _, h := range w.Hits {
		if v := toString(h[column]); v != "" {
			out = append(out, v)
		}
	}
	return out, nil
}

// ListServices lists the service names that reported spans in the last 24 hours.
func (t *Traces) ListServices(ctx context.Context) ([]string, error) {
	return t.distinct(ctx, "traces.listServices", "service_name", "")
}

// ListOperations lists the span names of service in the last 24 hours.
func (t *Traces) ListOperations(ctx context.Context, service string) ([]string, error) {
	if service == "" {
		return nil, ValidationError("traces.listOperations", "service is required")
	}
	return t.distinct(ctx, "traces.listOperations", "operation_name", "service_name = "+sqlString(service))
}

// GetDependencies returns UNSUPPORTED: the service graph is an enterprise
// feature that the open-source OpenObserve build does not serve.
func (t *Traces) GetDependencies(context.Context, DependencyOptions) ([]DependencyLink, error) {
	return nil, UnsupportedError("traces.getDependencies", "the service graph is an OpenObserve enterprise feature")
}

// WaitForTrace polls GetTrace until the trace has at least MinSpans spans.
func (t *Traces) WaitForTrace(ctx context.Context, traceID string, opts WaitForTraceOptions) (*Trace, error) {
	return WaitForTrace(ctx, t, traceID, opts)
}

// TraceTimeRange is where and when the server's trace index saw a trace.
type TraceTimeRange struct {
	TraceID string `json:"traceId"`
	// Found is false when the index has no entry for the trace.
	Found bool `json:"found"`
	// Stream is the traces stream holding the trace.
	Stream string    `json:"stream,omitempty"`
	Start  time.Time `json:"start"`
	End    time.Time `json:"end"`
}

// TraceTimeRange looks a trace up in the server's trace index (GET
// /api/{org}/traces/time_range).
func (t *Traces) TraceTimeRange(ctx context.Context, traceID string) (TraceTimeRange, error) {
	const op = "traces.traceTimeRange"
	id, err := NormalizeTraceID(traceID)
	if err != nil {
		return TraceTimeRange{}, err
	}
	var body struct {
		Results []struct {
			Stream string `json:"stream"`
			Status string `json:"status"`
			Range  struct {
				StartTime json.Number `json:"start_time"`
				EndTime   json.Number `json:"end_time"`
			} `json:"range"`
		} `json:"results"`
	}
	err = t.c.getJSON(ctx, op, &request{Method: http.MethodGet, Path: t.c.orgPath("/traces/time_range"), Query: url.Values{"trace_id": {id}}}, &body)
	if err != nil {
		return TraceTimeRange{}, err
	}
	out := TraceTimeRange{TraceID: id}
	for _, r := range body.Results {
		if r.Status != "found" {
			continue
		}
		s, _ := toInt64(r.Range.StartTime)
		e, _ := toInt64(r.Range.EndTime)
		out = TraceTimeRange{TraceID: id, Found: true, Stream: r.Stream, Start: fromMicros(s), End: fromMicros(e)}
		break
	}
	return out, nil
}

// TraceExporterFactory returns the ExporterFactory that sends spans to
// OpenObserve: OTLP/HTTP protobuf to {url}/api/{org}/v1/traces (default) or
// OTLP/gRPC to port 5081 of the same host, with the Basic Authorization header,
// the stream-name header (Config.TracesStream) and, for gRPC, the organization
// header. Values set in TracingConfig (Endpoint, Headers, TLS) win.
func TraceExporterFactory(cfg Config) ExporterFactory {
	return func(ctx context.Context, tc TracingConfig) (sdktrace.SpanExporter, error) {
		tc, err := tracingConfigFor(cfg, tc)
		if err != nil {
			return nil, err
		}
		return NewOTLPExporter(ctx, tc)
	}
}

// tracingConfigFor fills endpoint, headers and TLS of tc from cfg.
func tracingConfigFor(cfg Config, tc TracingConfig) (TracingConfig, error) {
	if err := cfg.Validate(); err != nil {
		return tc, err
	}
	cfg = cfg.withDefaults()
	if tc.Protocol == ProtocolNative {
		return tc, ConfigError("tracing: OpenObserve takes OTLP (http/protobuf or grpc), not a native protocol")
	}
	base, _ := url.Parse(cfg.URL) // validated above
	if tc.Endpoint == "" {
		if tc.Protocol == ProtocolGRPC {
			u := url.URL{Scheme: base.Scheme, Host: net.JoinHostPort(base.Hostname(), DefaultOTLPGRPCPort)}
			tc.Endpoint = u.String()
		} else {
			tc.Endpoint = strings.TrimSuffix(cfg.URL, "/") + "/api/" + cfg.Org + "/v1/traces"
		}
	}
	headers := map[string]string{}
	set := func(k, v string) {
		for existing := range tc.Headers {
			if strings.EqualFold(existing, k) {
				return
			}
		}
		headers[k] = v
	}
	for k, v := range tc.Headers {
		headers[k] = v
	}
	set("Authorization", basicAuth(cfg))
	set("stream-name", normalizeStreamName(cfg.TracesStream))
	if tc.Protocol == ProtocolGRPC {
		set("organization", cfg.Org)
	}
	tc.Headers = headers
	if tc.TLS == (TLSConfig{}) {
		tc.TLS = cfg.TLS
	}
	return tc, nil
}

// InitTracing starts OpenTelemetry tracing that exports to OpenObserve (see
// TraceExporterFactory) and returns the handle; call Shutdown before exit.
func InitTracing(ctx context.Context, cfg Config, tc TracingConfig) (*TracingHandle, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return InitTracingWith(ctx, tc, TraceExporterFactory(cfg))
}

// InitTracing starts tracing with the client's configuration (see the
// package-level InitTracing).
func (c *Client) InitTracing(ctx context.Context, tc TracingConfig) (*TracingHandle, error) {
	return InitTracing(ctx, c.cfg, tc)
}
