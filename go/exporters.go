package openobserve

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
)

// JSONLogExporter is a LogExporter that writes records to an OpenObserve stream
// through the _json ingest API. Each record becomes one row: _timestamp (µs),
// body, severity (text), severity_number, trace_id, span_id, trace_flags, the
// record attributes and the resource keys flattened with "_" (service.name →
// service_name).
type JSONLogExporter struct {
	c      *Client
	stream string
}

var _ LogExporter = (*JSONLogExporter)(nil)

// NewJSONLogExporter returns a _json exporter for stream ("" = Config.Stream).
func (c *Client) NewJSONLogExporter(stream string) *JSONLogExporter {
	if stream == "" {
		stream = c.cfg.Stream
	}
	return &JSONLogExporter{c: c, stream: normalizeStreamName(stream)}
}

// recordRow converts a record to an ingest row.
func recordRow(r LogRecord) map[string]any {
	row := make(map[string]any, len(r.Attributes)+len(r.Resource)+8)
	for k, v := range r.Attributes {
		row[k] = v
	}
	for k, v := range r.Resource {
		row[normalizeKey(k)] = v
	}
	ts := r.Timestamp
	row["_timestamp"] = ts.UnixMicro()
	row["body"] = r.Body
	sevText := r.SeverityText
	if sevText == "" {
		sevText = r.Severity.String()
	}
	row["severity"] = sevText
	row["severity_number"] = int(r.Severity)
	if r.TraceID != "" {
		row["trace_id"] = r.TraceID
	}
	if r.SpanID != "" {
		row["span_id"] = r.SpanID
	}
	if r.TraceFlags != nil {
		row["trace_flags"] = int(*r.TraceFlags)
	}
	return row
}

// Export sends one batch. Records the server rejects (for example outside the
// ingest window) are reported in ExportResult.Rejected with a VALIDATION error.
func (e *JSONLogExporter) Export(ctx context.Context, records []LogRecord, opts ExportOptions) (ExportResult, error) {
	const op = "logs.export"
	rows := make([]map[string]any, len(records))
	for i, r := range records {
		rows[i] = recordRow(r)
	}
	body, err := json.Marshal(rows)
	if err != nil {
		return ExportResult{}, ValidationError(op, "records cannot be encoded as JSON")
	}
	req := &request{Method: http.MethodPost, Path: e.c.orgPath("/" + e.stream + "/_json"), Body: body, ContentType: "application/json", Operation: op}
	compress(req, opts.Gzip)
	res, err := e.c.do(ctx, req)
	if err != nil {
		return ExportResult{}, err
	}
	ir, err := ingestResult(op, res)
	if err != nil {
		return ExportResult{}, err
	}
	out := ExportResult{Accepted: ir.Successful, Rejected: ir.Failed}
	if ir.Failed > 0 {
		reason := "records rejected"
		for _, s := range ir.Streams {
			if s.Error != "" {
				reason = s.Error
			}
		}
		out.Error = ValidationError(op, fmt.Sprintf("%d of %d records rejected: %s", ir.Failed, len(records), e.c.transport.redact(reason)))
	}
	return out, nil
}

// Shutdown implements LogExporter (nothing to release).
func (e *JSONLogExporter) Shutdown(context.Context) error { return nil }

// compress gzips the body of req when enabled.
func compress(req *request, enabled bool) {
	if !enabled {
		return
	}
	req.Body = gzipBytes(req.Body)
	if req.Header == nil {
		req.Header = http.Header{}
	}
	req.Header.Set("Content-Encoding", "gzip")
}

// OTLPLogExporter is a LogExporter that sends OTLP/HTTP JSON
// (ExportLogsServiceRequest) to /api/{org}/v1/logs with a stream-name header.
// Stored rows use the OTLP column names (body, severity, severity_number,
// trace_id, span_id, flattened attributes and resource); trace flags are not
// stored by OpenObserve.
type OTLPLogExporter struct {
	c      *Client
	stream string
}

var _ LogExporter = (*OTLPLogExporter)(nil)

// NewOTLPLogExporter returns an OTLP logs exporter for stream ("" = Config.Stream).
func (c *Client) NewOTLPLogExporter(stream string) *OTLPLogExporter {
	if stream == "" {
		stream = c.cfg.Stream
	}
	return &OTLPLogExporter{c: c, stream: normalizeStreamName(stream)}
}

// Export sends one batch as OTLP JSON. A partialSuccess answer is reported as
// rejected records with a VALIDATION error.
func (e *OTLPLogExporter) Export(ctx context.Context, records []LogRecord, opts ExportOptions) (ExportResult, error) {
	const op = "logs.exportOtlp"
	body, err := json.Marshal(otlpLogsRequest(records))
	if err != nil {
		return ExportResult{}, ValidationError(op, "records cannot be encoded as OTLP JSON")
	}
	req := &request{
		Method: http.MethodPost, Path: e.c.orgPath("/v1/logs"), Body: body, ContentType: "application/json",
		Header: http.Header{"Stream-Name": {e.stream}}, Operation: op,
	}
	compress(req, opts.Gzip)
	res, err := e.c.do(ctx, req)
	if err != nil {
		return ExportResult{}, err
	}
	out := ExportResult{Accepted: len(records)}
	var body2 struct {
		PartialSuccess *struct {
			RejectedLogRecords json.Number `json:"rejectedLogRecords"`
			ErrorMessage       string      `json:"errorMessage"`
		} `json:"partialSuccess"`
	}
	if len(res.Body) > 0 && json.Unmarshal(res.Body, &body2) == nil && body2.PartialSuccess != nil {
		n, _ := toInt64(body2.PartialSuccess.RejectedLogRecords)
		if n > 0 {
			rejected := int(min(n, int64(len(records))))
			out.Accepted, out.Rejected = len(records)-rejected, rejected
			out.Error = ValidationError(op, fmt.Sprintf("%d of %d records rejected: %s", rejected, len(records), e.c.transport.redact(body2.PartialSuccess.ErrorMessage)))
		}
	}
	return out, nil
}

// Shutdown implements LogExporter (nothing to release).
func (e *OTLPLogExporter) Shutdown(context.Context) error { return nil }

// otlpLogsRequest groups records by resource into an OTLP/JSON request.
func otlpLogsRequest(records []LogRecord) map[string]any {
	type group struct {
		resource map[string]AttrValue
		logs     []any
	}
	var order []string
	groups := map[string]*group{}
	for _, r := range records {
		key, _ := json.Marshal(r.Resource)
		g, ok := groups[string(key)]
		if !ok {
			g = &group{resource: r.Resource}
			groups[string(key)] = g
			order = append(order, string(key))
		}
		lr := map[string]any{
			"timeUnixNano":   strconv.FormatInt(r.Timestamp.UnixNano(), 10),
			"severityNumber": int(r.Severity),
			"severityText":   r.SeverityText,
			"body":           map[string]any{"stringValue": r.Body},
			"attributes":     otlpKeyValues(r.Attributes),
		}
		if lr["severityText"] == "" {
			lr["severityText"] = r.Severity.String()
		}
		if !r.ObservedTimestamp.IsZero() {
			lr["observedTimeUnixNano"] = strconv.FormatInt(r.ObservedTimestamp.UnixNano(), 10)
		}
		if isHex(r.TraceID, 32) {
			lr["traceId"] = r.TraceID
		}
		if isHex(r.SpanID, 16) {
			lr["spanId"] = r.SpanID
		}
		if r.TraceFlags != nil {
			lr["flags"] = int(*r.TraceFlags)
		}
		g.logs = append(g.logs, lr)
	}
	resourceLogs := make([]any, 0, len(order))
	for _, k := range order {
		g := groups[k]
		resourceLogs = append(resourceLogs, map[string]any{
			"resource":  map[string]any{"attributes": otlpKeyValues(g.resource)},
			"scopeLogs": []any{map[string]any{"scope": map[string]any{"name": "grow-codes-openobserve", "version": Version}, "logRecords": g.logs}},
		})
	}
	return map[string]any{"resourceLogs": resourceLogs}
}

func isHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// otlpKeyValues converts attributes to OTLP KeyValue objects, sorted by key.
func otlpKeyValues(attrs map[string]AttrValue) []any {
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{"key": k, "value": otlpAnyValue(attrs[k])})
	}
	return out
}

// otlpAnyValue converts an attribute value to an OTLP AnyValue (JSON form).
func otlpAnyValue(v AttrValue) map[string]any {
	switch x := NormalizeAttrValue(v).(type) {
	case string:
		return map[string]any{"stringValue": x}
	case bool:
		return map[string]any{"boolValue": x}
	case int64:
		return map[string]any{"intValue": strconv.FormatInt(x, 10)}
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return map[string]any{"stringValue": fmt.Sprint(x)}
		}
		return map[string]any{"doubleValue": x}
	case []AttrValue:
		values := make([]any, len(x))
		for i, e := range x {
			values[i] = otlpAnyValue(e)
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}
	case map[string]AttrValue:
		return map[string]any{"kvlistValue": map[string]any{"values": otlpKeyValues(x)}}
	}
	return map[string]any{"stringValue": ""}
}

// NewLogClient starts a batching LogClient that writes to stream ("" =
// Config.Stream) through the _json exporter. Client.Close flushes and closes it.
func (c *Client) NewLogClient(stream string, opts LogClientOptions) (*BatchingLogClient, error) {
	return c.NewLogClientWith(c.NewJSONLogExporter(stream), opts)
}

// NewLogClientWith starts a batching LogClient over exporter (for example
// NewOTLPLogExporter). Client.Close flushes and closes it.
func (c *Client) NewLogClientWith(exporter LogExporter, opts LogClientOptions) (*BatchingLogClient, error) {
	if exporter == nil {
		return nil, ConfigError("log exporter is required")
	}
	lc, err := NewBatchingLogClient(exporter, opts)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.logClients = append(c.logClients, lc)
	c.mu.Unlock()
	return lc, nil
}
