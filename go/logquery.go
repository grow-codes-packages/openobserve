package openobserve

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
)

// LogQuery implements LogQueryClient for one OpenObserve log stream.
//
// A query that starts with SELECT (case-insensitive) is raw SQL; any other
// query is a full-text search, match_all('<query>'), on the stream; an empty
// query matches every record. Results are normalized into LogRecord values:
// _timestamp, body or message, severity or level (+ severity_number), trace_id,
// span_id, trace_flags and the resource keys service_name, service_version,
// deployment_environment and host_name; every other column becomes an attribute.
type LogQuery struct {
	c      *Client
	stream string
}

var _ LogQueryClient = (*LogQuery)(nil)

// LogQuery returns the LogQueryClient for stream ("" = Config.Stream).
func (c *Client) LogQuery(stream string) *LogQuery {
	if stream == "" {
		stream = c.cfg.Stream
	}
	return &LogQuery{c: c, stream: normalizeStreamName(stream)}
}

// Stream returns the (normalized) stream this client queries.
func (q *LogQuery) Stream() string { return q.stream }

// buildSQL turns a LogQueryClient query into SQL.
func (q *LogQuery) buildSQL(query string, order SortOrder) string {
	if isSQL(query) {
		return strings.TrimSpace(query)
	}
	sql := "SELECT * FROM " + sqlIdent(q.stream)
	if t := strings.TrimSpace(query); t != "" {
		sql += " WHERE match_all(" + sqlString(t) + ")"
	}
	dir := "DESC"
	if order == OrderAsc {
		dir = "ASC"
	}
	return sql + " ORDER BY _timestamp " + dir
}

// Search returns one page of matching records (default window: the last 15
// minutes, limit 100, newest first). The cursor is the row offset of the next
// page; NextCursor is empty on the last page. Total is not reported (use
// Logs.Search with TrackTotalHits).
func (q *LogQuery) Search(ctx context.Context, query string, opts SearchOptions) (LogSearchResult, error) {
	const op = "logs.search"
	opts = opts.WithDefaults()
	start, end, err := timeWindow(op, opts.Start, opts.End, DefaultSearchWindow)
	if err != nil {
		return LogSearchResult{}, err
	}
	from := 0
	if opts.Cursor != "" {
		n, err := strconv.Atoi(opts.Cursor)
		if err != nil || n < 0 {
			return LogSearchResult{}, ValidationError(op, "invalid cursor")
		}
		from = n
	}
	w, err := q.c.searchRaw(ctx, op, q.buildSQL(query, opts.Order), start, end, from, opts.Limit, false, StreamTypeLogs)
	if err != nil {
		return LogSearchResult{}, err
	}
	out := LogSearchResult{Records: make([]LogRecord, 0, len(w.Hits))}
	for _, h := range w.Hits {
		out.Records = append(out.Records, recordFromRow(NormalizeAttributes(h)))
	}
	if len(w.Hits) >= opts.Limit {
		out.NextCursor = strconv.Itoa(from + len(w.Hits))
	}
	return out, nil
}

// ListStreams lists the names of the log streams.
func (q *LogQuery) ListStreams(ctx context.Context) ([]string, error) {
	list, err := q.c.Streams().ListStreams(ctx, ListStreamsOptions{Type: StreamTypeLogs})
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(list.Streams))
	for _, s := range list.Streams {
		names = append(names, s.Name)
	}
	return names, nil
}

// ListLabels lists the field names of the stream's schema.
func (q *LogQuery) ListLabels(ctx context.Context) ([]string, error) {
	info, err := q.c.Streams().GetStreamSchema(ctx, q.stream, StreamTypeLogs)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(info.Schema))
	for _, f := range info.Schema {
		names = append(names, f.Name)
	}
	return names, nil
}

// WaitForLog polls Search until a record matches (see the package-level WaitForLog).
func (q *LogQuery) WaitForLog(ctx context.Context, query string, opts WaitForLogOptions) (LogRecord, error) {
	return WaitForLog(ctx, q, query, opts)
}

// TailOverlap is how far each tail poll reaches back before the cursor; rows in
// the overlap are de-duplicated by content.
const TailOverlap = time.Second

// Tail polls the stream for new matching records and calls onRecord for each
// one, oldest first, until the handle is stopped or ctx is done. OpenObserve has
// no live tail and no row id: each poll re-reads the last TailOverlap and drops
// rows already seen (exact duplicates within the same microsecond are therefore
// delivered once). Search errors are logged and polling continues.
func (q *LogQuery) Tail(ctx context.Context, query string, onRecord func(LogRecord), opts TailOptions) (TailHandle, error) {
	if onRecord == nil {
		return nil, ValidationError("logs.tail", "onRecord is required")
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = time.Second
	}
	floor := opts.Start
	if floor.IsZero() {
		floor = time.Now()
	}
	tctx, cancel := context.WithCancel(ctx)
	h := &tailHandle{cancel: cancel, done: make(chan struct{})}
	t := &tailer{q: q, sql: q.buildSQL(query, OrderAsc), onRecord: onRecord, cursor: floor.UnixMicro(), floor: floor.UnixMicro(), seen: map[[32]byte]int64{}}
	go func() {
		defer close(h.done)
		defer cancel()
		for {
			t.poll(tctx)
			if sleepContext(tctx, interval) != nil {
				return
			}
		}
	}()
	return h, nil
}

// tailPageSize is the page size of one tail poll.
const tailPageSize = 500

type tailer struct {
	q        *LogQuery
	sql      string
	onRecord func(LogRecord)
	cursor   int64 // newest delivered _timestamp (µs)
	floor    int64 // records before floor are never delivered
	seen     map[[32]byte]int64
}

func (t *tailer) poll(ctx context.Context) {
	start := fromMicros(t.cursor - TailOverlap.Microseconds())
	end := time.Now().Add(time.Second)
	for from := 0; ctx.Err() == nil; from += tailPageSize {
		w, err := t.q.c.searchRaw(ctx, "logs.tail", t.sql, start, end, from, tailPageSize, false, StreamTypeLogs)
		if err != nil {
			if ctx.Err() == nil {
				t.q.c.transport.log(slog.LevelDebug, "tail poll failed", slog.String("error", err.Error()))
			}
			return
		}
		for _, raw := range w.Hits {
			t.deliver(NormalizeAttributes(raw))
		}
		if len(w.Hits) < tailPageSize {
			break
		}
	}
	limit := t.cursor - TailOverlap.Microseconds()
	for k, ts := range t.seen {
		if ts < limit {
			delete(t.seen, k)
		}
	}
}

func (t *tailer) deliver(row map[string]AttrValue) {
	ts, _ := toInt64(row["_timestamp"])
	if ts < t.floor {
		return
	}
	raw, _ := json.Marshal(row) // map keys are sorted: equal rows hash equally
	key := sha256.Sum256(raw)
	if _, dup := t.seen[key]; dup {
		return
	}
	t.seen[key] = ts
	if ts > t.cursor {
		t.cursor = ts
	}
	safeCall(func() { t.onRecord(recordFromRow(row)) })
}

type tailHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// Stop ends the tail and waits for the poller to exit. It is idempotent.
func (h *tailHandle) Stop() {
	h.once.Do(h.cancel)
	<-h.done
}

// resourceColumns maps stored columns to resource attribute keys.
var resourceColumns = map[string]string{
	"service_name":           "service.name",
	"service_version":        "service.version",
	"deployment_environment": "deployment.environment",
	"host_name":              "host.name",
}

// recordFromRow normalizes a stored log row into a LogRecord.
func recordFromRow(row map[string]AttrValue) LogRecord {
	rec := LogRecord{Attributes: map[string]AttrValue{}, Resource: map[string]AttrValue{}}
	used := map[string]bool{"_timestamp": true, "_o2_ingest_ts": true}
	if ts, ok := toInt64(row["_timestamp"]); ok {
		rec.Timestamp = fromMicros(ts)
	}
	for _, k := range []string{"body", "message"} {
		if v, ok := row[k]; ok {
			rec.Body, used[k] = toString(v), true
			break
		}
	}
	for _, k := range []string{"severity", "level"} {
		if v, ok := row[k]; ok {
			rec.SeverityText, used[k] = toString(v), true
			break
		}
	}
	if n, ok := toInt64(row["severity_number"]); ok && n >= 1 && n <= 24 {
		rec.Severity = Severity(n)
		used["severity_number"] = true
	} else if s, ok := ParseSeverity(rec.SeverityText); ok {
		rec.Severity = s
	}
	if rec.SeverityText == "" && rec.Severity != SeverityUnspecified {
		rec.SeverityText = rec.Severity.String()
	}
	if v, ok := row["trace_id"]; ok {
		rec.TraceID, used["trace_id"] = strings.ToLower(toString(v)), true
	}
	if v, ok := row["span_id"]; ok {
		rec.SpanID, used["span_id"] = strings.ToLower(toString(v)), true
	}
	if n, ok := toInt64(row["trace_flags"]); ok && n >= 0 && n <= 255 {
		f := uint8(n)
		rec.TraceFlags, used["trace_flags"] = &f, true
	}
	for col, key := range resourceColumns {
		if v, ok := row[col]; ok {
			rec.Resource[key], used[col] = v, true
		}
	}
	for k, v := range row {
		if !used[k] {
			rec.Attributes[k] = v
		}
	}
	return rec
}
