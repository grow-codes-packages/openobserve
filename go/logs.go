package openobserve

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Logs is the log ingestion and SQL search API of OpenObserve (Client.Logs).
type Logs struct{ c *Client }

// Logs returns the log ingestion and search API.
func (c *Client) Logs() *Logs { return &Logs{c: c} }

// StreamType is the type of an OpenObserve stream.
type StreamType string

// Stream types.
const (
	StreamTypeLogs     StreamType = "logs"
	StreamTypeTraces   StreamType = "traces"
	StreamTypeMetrics  StreamType = "metrics"
	StreamTypeMetadata StreamType = "metadata"
)

func (t StreamType) orLogs() StreamType {
	if t == "" {
		return StreamTypeLogs
	}
	return t
}

// IngestStreamStatus is the per-stream outcome of an ingest call.
type IngestStreamStatus struct {
	// Name is the stream name as normalized by the server.
	Name string `json:"name"`
	// Successful is the number of records stored.
	Successful int `json:"successful"`
	// Failed is the number of records rejected (for example older than the
	// ingest window).
	Failed int `json:"failed"`
	// Error is the last rejection reason the server kept for this stream.
	Error string `json:"error,omitempty"`
}

// IngestResult is the outcome of IngestJSON / IngestMulti. OpenObserve answers
// HTTP 200 even when some records were rejected: check Failed.
type IngestResult struct {
	Successful int                  `json:"successful"`
	Failed     int                  `json:"failed"`
	Streams    []IngestStreamStatus `json:"streams"`
}

func ingestResult(op string, res *response) (IngestResult, error) {
	var body struct {
		Status []IngestStreamStatus `json:"status"`
	}
	if err := decodeBody(op, res, &body); err != nil {
		return IngestResult{}, err
	}
	out := IngestResult{Streams: body.Status}
	if out.Streams == nil {
		out.Streams = []IngestStreamStatus{}
	}
	for _, s := range body.Status {
		out.Successful += s.Successful
		out.Failed += s.Failed
	}
	return out, nil
}

// IngestJSON stores records in stream (POST /api/{org}/{stream}/_json). A
// "_timestamp" field may hold Unix microseconds (milliseconds, nanoseconds and
// RFC 3339 strings are detected by the server); without it the server time is
// used. Nested objects are flattened with "_" by the server.
func (l *Logs) IngestJSON(ctx context.Context, stream string, records []map[string]any) (IngestResult, error) {
	const op = "logs.ingestJson"
	s, err := streamArg(op, stream)
	if err != nil {
		return IngestResult{}, err
	}
	if len(records) == 0 {
		return IngestResult{}, ValidationError(op, "at least one record is required")
	}
	body, err := json.Marshal(records)
	if err != nil {
		return IngestResult{}, ValidationError(op, "records cannot be encoded as JSON")
	}
	res, err := l.c.do(ctx, &request{Method: http.MethodPost, Path: l.c.orgPath("/" + s + "/_json"), Body: body, ContentType: "application/json", Operation: op})
	if err != nil {
		return IngestResult{}, err
	}
	return ingestResult(op, res)
}

// IngestMulti stores newline-delimited JSON objects in stream (POST
// /api/{org}/{stream}/_multi). One invalid line rejects the whole request with a
// VALIDATION error.
func (l *Logs) IngestMulti(ctx context.Context, stream string, ndjson []byte) (IngestResult, error) {
	const op = "logs.ingestMulti"
	s, err := streamArg(op, stream)
	if err != nil {
		return IngestResult{}, err
	}
	if len(bytes.TrimSpace(ndjson)) == 0 {
		return IngestResult{}, ValidationError(op, "ndjson body is empty")
	}
	res, err := l.c.do(ctx, &request{Method: http.MethodPost, Path: l.c.orgPath("/" + s + "/_multi"), Body: ndjson, ContentType: "application/x-ndjson", Operation: op})
	if err != nil {
		return IngestResult{}, err
	}
	return ingestResult(op, res)
}

// Bulk actions accepted by IngestBulk. OpenObserve silently ignores delete and
// update actions, so the SDK rejects them with a VALIDATION error.
const (
	BulkIndex  = "index"
	BulkCreate = "create"
)

// BulkAction is one Elasticsearch-style bulk action.
type BulkAction struct {
	// Action is BulkIndex (default) or BulkCreate.
	Action string
	// Index is the target stream.
	Index string
	// Document is the record.
	Document map[string]any
}

// BulkItem is the outcome of one bulk action.
type BulkItem struct {
	Action string `json:"action"`
	Index  string `json:"index"`
	// Status is the per-item HTTP status (200 stored, 4xx rejected).
	Status int `json:"status"`
	// Error is "<type>: <reason>" for a rejected item.
	Error string `json:"error,omitempty"`
}

// BulkResult is the outcome of IngestBulk. Errors is true when at least one
// item was rejected (the HTTP status is still 200).
type BulkResult struct {
	Took   int        `json:"took"`
	Errors bool       `json:"errors"`
	Items  []BulkItem `json:"items"`
}

// IngestBulk sends Elasticsearch-compatible bulk actions (POST /api/{org}/_bulk).
func (l *Logs) IngestBulk(ctx context.Context, actions []BulkAction) (BulkResult, error) {
	const op = "logs.ingestBulk"
	if len(actions) == 0 {
		return BulkResult{}, ValidationError(op, "at least one action is required")
	}
	var buf bytes.Buffer
	for i, a := range actions {
		action := a.Action
		if action == "" {
			action = BulkIndex
		}
		if action != BulkIndex && action != BulkCreate {
			return BulkResult{}, ValidationError(op, fmt.Sprintf("action %d: only %q and %q are supported (OpenObserve ignores %q)", i, BulkIndex, BulkCreate, action))
		}
		index, err := streamArg(op, a.Index)
		if err != nil {
			return BulkResult{}, err
		}
		head, _ := json.Marshal(map[string]map[string]string{action: {"_index": index}})
		doc, err := json.Marshal(a.Document)
		if err != nil || a.Document == nil {
			return BulkResult{}, ValidationError(op, fmt.Sprintf("action %d: document must be a JSON object", i))
		}
		buf.Write(head)
		buf.WriteByte('\n')
		buf.Write(doc)
		buf.WriteByte('\n')
	}
	res, err := l.c.do(ctx, &request{Method: http.MethodPost, Path: l.c.orgPath("/_bulk"), Body: buf.Bytes(), ContentType: "application/x-ndjson", Operation: op})
	if err != nil {
		return BulkResult{}, err
	}
	var body struct {
		Took   int                                     `json:"took"`
		Errors bool                                    `json:"errors"`
		Items  []map[string]map[string]json.RawMessage `json:"items"`
	}
	if err := decodeBody(op, res, &body); err != nil {
		return BulkResult{}, err
	}
	out := BulkResult{Took: body.Took, Errors: body.Errors, Items: make([]BulkItem, 0, len(body.Items))}
	for _, item := range body.Items {
		for action, fields := range item {
			bi := BulkItem{Action: action}
			_ = json.Unmarshal(fields["_index"], &bi.Index)
			_ = json.Unmarshal(fields["status"], &bi.Status)
			var e struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			}
			if json.Unmarshal(fields["error"], &e) == nil && (e.Type != "" || e.Reason != "") {
				bi.Error = strings.TrimPrefix(e.Type+": "+e.Reason, ": ")
			}
			out.Items = append(out.Items, bi)
		}
	}
	return out, nil
}

// SQLSearchOptions narrow Logs.Search. Times default to the last 15 minutes.
type SQLSearchOptions struct {
	// Start and End bound the search (Unix microseconds on the wire).
	Start time.Time
	End   time.Time
	// From is the row offset; Size the page size (0 = server default). A LIMIT in
	// the SQL overrides Size.
	From int
	Size int
	// TrackTotalHits also runs a count request and fills SQLSearchResult.Total.
	TrackTotalHits bool
	// StreamType is the type of the queried stream (default logs).
	StreamType StreamType
}

// SQLSearchResult is one page of an OpenObserve SQL search.
type SQLSearchResult struct {
	// Hits are the result rows (flat objects; numbers are int64 or float64).
	Hits []map[string]any `json:"hits"`
	// Total is the number of matching rows; set only with TrackTotalHits (the
	// server's own "total" is the page length).
	Total *int `json:"total,omitempty"`
	From  int  `json:"from"`
	Size  int  `json:"size"`
	// Took is the server time in milliseconds.
	Took int `json:"took"`
	// ScanRecords is the number of records the server scanned.
	ScanRecords int64 `json:"scanRecords"`
	// SearchID is the server's search trace id (not telemetry).
	SearchID string `json:"searchId,omitempty"`
	// IsPartial reports a partial result.
	IsPartial bool `json:"isPartial"`
	// HistogramInterval is the bucket width in seconds for histogram queries.
	HistogramInterval int `json:"histogramInterval,omitempty"`
}

type wireSearch struct {
	Hits              []map[string]any `json:"hits"`
	From              int              `json:"from"`
	Size              int              `json:"size"`
	Took              int              `json:"took"`
	ScanRecords       int64            `json:"scan_records"`
	TraceID           string           `json:"trace_id"`
	IsPartial         bool             `json:"is_partial"`
	HistogramInterval int              `json:"histogram_interval"`
}

func (w wireSearch) result() SQLSearchResult {
	out := SQLSearchResult{
		Hits: make([]map[string]any, 0, len(w.Hits)), From: w.From, Size: w.Size, Took: w.Took,
		ScanRecords: w.ScanRecords, SearchID: w.TraceID, IsPartial: w.IsPartial, HistogramInterval: w.HistogramInterval,
	}
	for _, h := range w.Hits {
		out.Hits = append(out.Hits, NormalizeAttributes(h))
	}
	return out
}

// searchRaw posts one _search request.
func (c *Client) searchRaw(ctx context.Context, op, sql string, start, end time.Time, from, size int, track bool, typ StreamType) (wireSearch, error) {
	q := map[string]any{"sql": sql, "start_time": start.UnixMicro(), "end_time": end.UnixMicro(), "from": from, "size": size}
	if track {
		q["track_total_hits"] = true
	}
	var w wireSearch
	err := c.getJSON(ctx, op, &request{
		Method: http.MethodPost, Path: c.orgPath("/_search"), Query: url.Values{"type": {string(typ.orLogs())}},
		JSON: map[string]any{"query": q}, Idempotent: true,
	}, &w)
	return w, err
}

// Search runs an OpenObserve SQL query (POST /api/{org}/_search). Find text with
// match_all('x'), str_match(field, 'x') or re_match(field, 'regex'); quote
// reserved stream names ("default").
func (l *Logs) Search(ctx context.Context, sql string, opts SQLSearchOptions) (SQLSearchResult, error) {
	const op = "logs.search"
	if strings.TrimSpace(sql) == "" {
		return SQLSearchResult{}, ValidationError(op, "sql is required")
	}
	start, end, err := timeWindow(op, opts.Start, opts.End, DefaultSearchWindow)
	if err != nil {
		return SQLSearchResult{}, err
	}
	w, err := l.c.searchRaw(ctx, op, sql, start, end, opts.From, opts.Size, false, opts.StreamType)
	if err != nil {
		return SQLSearchResult{}, err
	}
	out := w.result()
	if opts.TrackTotalHits {
		cw, err := l.c.searchRaw(ctx, op, sql, start, end, 0, 0, true, opts.StreamType)
		if err != nil {
			return SQLSearchResult{}, err
		}
		total := 0
		if len(cw.Hits) > 0 {
			n, _ := toInt64(cw.Hits[0]["zo_sql_num"])
			total = int(n)
		}
		out.Total = &total
	}
	return out, nil
}

// SearchAround returns up to size rows of stream around timestamp (GET
// /api/{org}/{stream}/_around), the anchor row included.
func (l *Logs) SearchAround(ctx context.Context, stream string, timestamp time.Time, size int) (SQLSearchResult, error) {
	const op = "logs.searchAround"
	s, err := streamArg(op, stream)
	if err != nil {
		return SQLSearchResult{}, err
	}
	if timestamp.IsZero() {
		return SQLSearchResult{}, ValidationError(op, "timestamp is required")
	}
	if size <= 0 {
		size = 10
	}
	var w wireSearch
	err = l.c.getJSON(ctx, op, &request{
		Method: http.MethodGet, Path: l.c.orgPath("/" + s + "/_around"),
		Query: url.Values{"key": {micros(timestamp)}, "size": {strconv.Itoa(size)}, "type": {string(StreamTypeLogs)}},
	}, &w)
	if err != nil {
		return SQLSearchResult{}, err
	}
	return w.result(), nil
}

// ValuesOptions narrow Logs.ValuesOf. Times default to the last 15 minutes.
type ValuesOptions struct {
	Start time.Time
	End   time.Time
	// Size is the maximum number of distinct values per field (default 10).
	Size int
}

// ValueCount is one distinct value with its number of records.
type ValueCount struct {
	Value AttrValue `json:"value"`
	Count int64     `json:"count"`
}

// FieldValues lists the most frequent values of one field.
type FieldValues struct {
	Field  string       `json:"field"`
	Values []ValueCount `json:"values"`
}

// ValuesOf returns the most frequent values of fields in stream (GET
// /api/{org}/{stream}/_values).
func (l *Logs) ValuesOf(ctx context.Context, stream string, fields []string, opts ValuesOptions) ([]FieldValues, error) {
	const op = "logs.valuesOf"
	s, err := streamArg(op, stream)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, ValidationError(op, "at least one field is required")
	}
	start, end, err := timeWindow(op, opts.Start, opts.End, DefaultSearchWindow)
	if err != nil {
		return nil, err
	}
	size := opts.Size
	if size <= 0 {
		size = 10
	}
	var body struct {
		Hits []struct {
			Field  string           `json:"field"`
			Values []map[string]any `json:"values"`
		} `json:"hits"`
	}
	err = l.c.getJSON(ctx, op, &request{
		Method: http.MethodGet, Path: l.c.orgPath("/" + s + "/_values"),
		Query: url.Values{"fields": {strings.Join(fields, ",")}, "start_time": {micros(start)}, "end_time": {micros(end)}, "size": {strconv.Itoa(size)}, "type": {string(StreamTypeLogs)}},
	}, &body)
	if err != nil {
		return nil, err
	}
	out := make([]FieldValues, 0, len(body.Hits))
	for _, h := range body.Hits {
		fv := FieldValues{Field: h.Field, Values: make([]ValueCount, 0, len(h.Values))}
		for _, v := range h.Values {
			n, _ := toInt64(v["zo_sql_num"])
			fv.Values = append(fv.Values, ValueCount{Value: NormalizeAttrValue(v["zo_sql_key"]), Count: n})
		}
		out = append(out, fv)
	}
	return out, nil
}

// HistogramOptions narrow Logs.Histogram. Times default to the last 15 minutes.
type HistogramOptions struct {
	Start time.Time
	End   time.Time
	// Interval is the bucket width (0 = chosen by the server).
	Interval time.Duration
	// Filter is an SQL WHERE condition, for example "level = 'error'".
	Filter string
}

// HistogramBucket is one time bucket.
type HistogramBucket struct {
	// Start is the bucket start (UTC).
	Start time.Time `json:"start"`
	Count int64     `json:"count"`
}

// HistogramResult is the record count per time bucket.
type HistogramResult struct {
	// Interval is the bucket width reported by the server.
	Interval time.Duration     `json:"interval"`
	Buckets  []HistogramBucket `json:"buckets"`
}

// intervalText renders d as an SQL interval ("5 minute", "30 second").
func intervalText(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%d hour", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%d minute", d/time.Minute)
	}
	return fmt.Sprintf("%d second", max(int64(d/time.Second), 1))
}

// Histogram counts records of stream per time bucket (SQL histogram() through
// _search).
func (l *Logs) Histogram(ctx context.Context, stream string, opts HistogramOptions) (HistogramResult, error) {
	const op = "logs.histogram"
	s, err := streamArg(op, stream)
	if err != nil {
		return HistogramResult{}, err
	}
	start, end, err := timeWindow(op, opts.Start, opts.End, DefaultSearchWindow)
	if err != nil {
		return HistogramResult{}, err
	}
	if opts.Interval < 0 {
		return HistogramResult{}, ValidationError(op, "interval must not be negative")
	}
	hist := "histogram(_timestamp)"
	if opts.Interval > 0 {
		hist = "histogram(_timestamp, " + sqlString(intervalText(opts.Interval)) + ")"
	}
	sql := "SELECT " + hist + " AS zo_sql_key, count(*) AS zo_sql_num FROM " + sqlIdent(s)
	if f := strings.TrimSpace(opts.Filter); f != "" {
		sql += " WHERE " + f
	}
	sql += " GROUP BY zo_sql_key ORDER BY zo_sql_key"
	w, err := l.c.searchRaw(ctx, op, sql, start, end, 0, 0, false, StreamTypeLogs)
	if err != nil {
		return HistogramResult{}, err
	}
	out := HistogramResult{Interval: time.Duration(w.HistogramInterval) * time.Second, Buckets: make([]HistogramBucket, 0, len(w.Hits))}
	for _, h := range w.Hits {
		n, _ := toInt64(h["zo_sql_num"])
		out.Buckets = append(out.Buckets, HistogramBucket{Start: parseBucketTime(toString(h["zo_sql_key"])), Count: n})
	}
	return out, nil
}

// parseBucketTime parses a histogram key (naive UTC "2026-11-18T20:03:00", RFC
// 3339 or Unix microseconds).
func parseBucketTime(s string) time.Time {
	for _, layout := range []string{"2006-01-02T15:04:05", time.RFC3339Nano, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	if us, err := strconv.ParseInt(s, 10, 64); err == nil {
		return fromMicros(us)
	}
	return time.Time{}
}
