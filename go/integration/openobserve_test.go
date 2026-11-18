package integration

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	openobserve "github.com/grow-codes-packages/openobserve/go"
)

// setup loads the bootstrap environment and returns a client plus a run id.
func setup(t *testing.T) (*openobserve.Client, string) {
	t.Helper()
	bootstrap := os.Getenv("GROW_BOOTSTRAP_ENV")
	if bootstrap == "" {
		bootstrap = "/bootstrap/env"
	}
	loadBootstrapEnv(t, bootstrap)
	if os.Getenv("GROW_OPENOBSERVE_URL") == "" {
		t.Skip("GROW_OPENOBSERVE_URL is not set; run through docker/compose.yml")
	}
	c, err := openobserve.NewClientFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return c, hex.EncodeToString(b)
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// result carries a (value, error) pair: must(f()).get(t) fails the test on error.
type result[T any] struct {
	v   T
	err error
}

func must[T any](v T, err error) result[T] { return result[T]{v, err} }

func (r result[T]) get(t *testing.T) T {
	t.Helper()
	if r.err != nil {
		t.Fatal(r.err)
	}
	return r.v
}

// noErr fails the test on err.
func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCore(t *testing.T) {
	c, _ := setup(t)
	ctx := ctxT(t)
	hs := must(c.Health(ctx)).get(t)
	if !hs.Healthy || hs.Status != "ok" {
		t.Fatalf("health %+v", hs)
	}
	if d := must(c.Ping(ctx)).get(t); d <= 0 {
		t.Fatal("ping latency")
	}
	v := must(c.ServerVersion(ctx)).get(t)
	if want := os.Getenv("OPENOBSERVE_SERVER_VERSION"); v == "" || (want != "" && v != want) {
		t.Fatalf("version %q, bootstrap %q", v, want)
	}
	cfg := c.Config()
	if strings.Contains(cfg.String(), cfg.Password.Reveal()) {
		t.Fatal("config leaks the password")
	}

	t.Run("service-account token and pre-encoded header", func(t *testing.T) {
		email, token := os.Getenv("OPENOBSERVE_SA_EMAIL"), os.Getenv("OPENOBSERVE_SA_TOKEN")
		if email == "" || token == "" {
			t.Skip("no SA credentials in bootstrap env")
		}
		sa := must(openobserve.NewClient(openobserve.Config{CommonConfig: openobserve.CommonConfig{URL: cfg.URL}, User: email, Token: openobserve.NewSecret(token)})).get(t)
		defer func() { _ = sa.Close() }()
		must(sa.Ping(ctx)).get(t)
		header := "Basic " + base64.StdEncoding.EncodeToString([]byte(email+":"+token))
		pre := must(openobserve.NewClient(openobserve.Config{CommonConfig: openobserve.CommonConfig{URL: cfg.URL}, Token: openobserve.NewSecret(header)})).get(t)
		defer func() { _ = pre.Close() }()
		must(pre.Ping(ctx)).get(t)
	})
	t.Run("bad password is AUTHENTICATION and redacted", func(t *testing.T) {
		bad := must(openobserve.NewClient(openobserve.Config{CommonConfig: openobserve.CommonConfig{URL: cfg.URL}, User: cfg.User, Password: openobserve.NewSecret("wrong-PASSWORD-x")})).get(t)
		defer func() { _ = bad.Close() }()
		_, err := bad.Ping(ctx)
		if !errors.Is(err, openobserve.ErrAuthentication) || strings.Contains(err.Error(), "wrong-PASSWORD-x") {
			t.Fatal(err)
		}
	})
	t.Run("close is idempotent", func(t *testing.T) {
		c2 := must(openobserve.NewClientFromEnv()).get(t)
		if c2.Close() != nil || c2.Close() != nil {
			t.Fatal("close")
		}
		if _, err := c2.Ping(ctx); !errors.Is(err, openobserve.ErrCancelled) {
			t.Fatal(err)
		}
	})
}

func TestLogsIngestAndSearch(t *testing.T) {
	c, run := setup(t)
	ctx := ctxT(t)
	logs := c.Logs()
	stream := "go_it_" + run
	t.Cleanup(func() { _ = c.Streams().DeleteStream(context.Background(), stream, openobserve.StreamTypeLogs) })
	now := time.Now()
	us := func(d time.Duration) int64 { return now.Add(d).UnixMicro() }

	res := must(logs.IngestJSON(ctx, stream, []map[string]any{
		{"_timestamp": us(-3 * time.Second), "message": "alpha needle" + run, "level": "info", "run_id": run, "n": 1},
		{"_timestamp": us(-2 * time.Second), "message": "beta", "level": "error", "run_id": run, "user": map[string]any{"id": 7}},
		{"_timestamp": us(-1 * time.Second), "message": "gamma", "level": "warn", "run_id": run},
	})).get(t)
	if res.Successful != 3 || res.Failed != 0 || res.Streams[0].Name != stream {
		t.Fatalf("ingestJson %+v", res)
	}
	res = must(logs.IngestMulti(ctx, stream, []byte(fmt.Sprintf("{\"message\":\"multi 0\",\"level\":\"debug\",\"run_id\":%q}\n{\"message\":\"multi 1\",\"level\":\"debug\",\"run_id\":%q}\n", run, run)))).get(t)
	if res.Successful != 2 {
		t.Fatalf("ingestMulti %+v", res)
	}
	bulk := must(logs.IngestBulk(ctx, []openobserve.BulkAction{
		{Index: stream, Document: map[string]any{"message": "bulk one", "level": "info", "run_id": run}},
		{Action: openobserve.BulkCreate, Index: stream, Document: map[string]any{"message": "bulk two", "level": "info", "run_id": run}},
	})).get(t)
	if bulk.Errors || len(bulk.Items) != 2 || bulk.Items[0].Status != 200 {
		t.Fatalf("ingestBulk %+v", bulk)
	}
	if _, err := logs.IngestBulk(ctx, []openobserve.BulkAction{{Action: "delete", Index: stream, Document: map[string]any{}}}); !errors.Is(err, openobserve.ErrValidation) {
		t.Fatal("delete must be rejected client side:", err)
	}
	partial := must(logs.IngestJSON(ctx, stream, []map[string]any{{"message": "too old", "run_id": "old-" + run, "_timestamp": now.Add(-10 * time.Hour).UnixMicro()}})).get(t)
	if partial.Failed != 1 || partial.Successful != 0 || partial.Streams[0].Error == "" {
		t.Fatalf("partial failure %+v", partial)
	}
	bulkOld := must(logs.IngestBulk(ctx, []openobserve.BulkAction{{Index: stream, Document: map[string]any{"message": "old", "_timestamp": now.Add(-10 * time.Hour).UnixMicro()}}})).get(t)
	if !bulkOld.Errors || bulkOld.Items[0].Status < 400 || bulkOld.Items[0].Error == "" {
		t.Fatalf("bulk item error %+v", bulkOld)
	}

	sql := fmt.Sprintf(`SELECT * FROM "%s" WHERE run_id = '%s' ORDER BY _timestamp DESC`, stream, run)
	window := openobserve.SQLSearchOptions{Start: now.Add(-time.Minute), End: now.Add(time.Minute)}

	t.Run("search with total and paging", func(t *testing.T) {
		opts := window
		opts.Size, opts.TrackTotalHits = 100, true
		sr := waitSearch(t, ctx, logs, sql, opts, 7)
		if sr.Total == nil || *sr.Total != 7 || sr.SearchID == "" {
			t.Fatalf("total %+v", sr)
		}
		if _, ok := sr.Hits[0]["_timestamp"].(int64); !ok {
			t.Fatal("timestamps are int64")
		}
		flat := false
		for _, h := range sr.Hits {
			if h["user_id"] == int64(7) {
				flat = true
			}
		}
		if !flat {
			t.Fatal("nested objects are flattened to user_id")
		}
		opts = window
		opts.From, opts.Size = 1, 2
		page := must(logs.Search(ctx, sql, opts)).get(t)
		if len(page.Hits) != 2 || page.Hits[0]["message"] != sr.Hits[1]["message"] {
			t.Fatalf("paging %+v", page.Hits)
		}
	})
	t.Run("searchAround", func(t *testing.T) {
		around := must(logs.SearchAround(ctx, stream, time.UnixMicro(us(-2*time.Second)), 10)).get(t)
		found := false
		for _, h := range around.Hits {
			found = found || h["message"] == "beta"
		}
		if !found {
			t.Fatalf("anchor row missing: %+v", around.Hits)
		}
	})
	t.Run("valuesOf", func(t *testing.T) {
		vals := must(logs.ValuesOf(ctx, stream, []string{"level"}, openobserve.ValuesOptions{Start: window.Start, End: window.End, Size: 10})).get(t)
		counts := map[string]int64{}
		for _, v := range vals[0].Values {
			counts[fmt.Sprint(v.Value)] = v.Count
		}
		if vals[0].Field != "level" || counts["info"] != 3 || counts["debug"] != 2 || counts["error"] != 1 {
			t.Fatalf("values %+v", vals)
		}
	})
	t.Run("histogram", func(t *testing.T) {
		h := must(logs.Histogram(ctx, stream, openobserve.HistogramOptions{Start: window.Start, End: window.End, Interval: time.Minute, Filter: "run_id = '" + run + "'"})).get(t)
		var total int64
		for _, b := range h.Buckets {
			total += b.Count
			if b.Start.IsZero() {
				t.Fatal("bucket start")
			}
		}
		if total != 7 || h.Interval != time.Minute {
			t.Fatalf("histogram %+v", h)
		}
	})
	t.Run("missing stream is NOT_FOUND", func(t *testing.T) {
		if _, err := logs.Search(ctx, `SELECT * FROM "no_such_stream_`+run+`"`, window); !errors.Is(err, openobserve.ErrNotFound) {
			t.Fatal(err)
		}
	})

	t.Run("log query client", func(t *testing.T) {
		q := c.LogQuery(stream)
		rec := must(q.WaitForLog(ctx, "needle"+run, openobserve.WaitForLogOptions{Timeout: 30 * time.Second, Interval: 250 * time.Millisecond})).get(t)
		if rec.Body != "alpha needle"+run || rec.Severity != openobserve.SeverityInfo || rec.Attributes["run_id"] != run {
			t.Fatalf("record %+v", rec)
		}
		sr := must(q.Search(ctx, "SELECT * FROM \""+stream+"\" WHERE run_id = '"+run+"'", openobserve.SearchOptions{Limit: 3})).get(t)
		if len(sr.Records) != 3 || sr.NextCursor != "3" {
			t.Fatalf("raw SQL page %+v", sr)
		}
		next := must(q.Search(ctx, "SELECT * FROM \""+stream+"\" WHERE run_id = '"+run+"'", openobserve.SearchOptions{Limit: 3, Cursor: sr.NextCursor})).get(t)
		if len(next.Records) != 3 {
			t.Fatalf("second page %+v", next)
		}
		streams := must(q.ListStreams(ctx)).get(t)
		if !slices.Contains(streams, stream) {
			t.Fatalf("listStreams %v", streams)
		}
		labels := must(q.ListLabels(ctx)).get(t)
		if !slices.Contains(labels, "run_id") || !slices.Contains(labels, "message") {
			t.Fatalf("labels %v", labels)
		}
	})

	t.Run("tail", func(t *testing.T) {
		q := c.LogQuery(stream)
		var mu sync.Mutex
		var seen []string
		h := must(q.Tail(ctx, "tailed"+run, func(r openobserve.LogRecord) {
			mu.Lock()
			seen = append(seen, r.Body)
			mu.Unlock()
		}, openobserve.TailOptions{Interval: 200 * time.Millisecond})).get(t)
		defer h.Stop()
		time.Sleep(300 * time.Millisecond)
		must(logs.IngestJSON(ctx, stream, []map[string]any{{"message": "first tailed" + run}, {"message": "second tailed" + run}})).get(t)
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			n := len(seen)
			mu.Unlock()
			if n >= 2 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(600 * time.Millisecond) // a few more polls: the overlap must not re-deliver
		h.Stop()
		mu.Lock()
		defer mu.Unlock()
		if len(seen) != 2 {
			t.Fatalf("tail delivered %v", seen)
		}
	})
}

func waitSearch(t *testing.T, ctx context.Context, logs *openobserve.Logs, sql string, opts openobserve.SQLSearchOptions, n int) openobserve.SQLSearchResult {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		sr := must(logs.Search(ctx, sql, opts)).get(t)
		if len(sr.Hits) >= n || time.Now().After(deadline) {
			if len(sr.Hits) != n {
				t.Fatalf("want %d hits, got %d", n, len(sr.Hits))
			}
			return sr
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func TestLogClientsAndExporters(t *testing.T) {
	c, run := setup(t)
	ctx := ctxT(t)
	stream := "go_lc_" + run
	otlpStream := "go_otlp_" + run
	t.Cleanup(func() {
		_ = c.Streams().DeleteStream(context.Background(), stream, "")
		_ = c.Streams().DeleteStream(context.Background(), otlpStream, "")
	})
	traceID, spanID := "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"

	lc := must(c.NewLogClient(stream, openobserve.LogClientOptions{Resource: map[string]openobserve.AttrValue{"service.name": "go-it", "deployment.environment": "test"}})).get(t)
	lc.Send(ctx, openobserve.LogInput{Message: "json exporter " + run, Severity: openobserve.SeverityError, TraceID: traceID, SpanID: spanID,
		Attributes: map[string]openobserve.AttrValue{"run": run, "password": "hunter2-" + run}})
	lc.Logger("it", map[string]openobserve.AttrValue{"component": "logger"}).Warn(ctx, "logger line "+run, "k", "v")
	slog.New(openobserve.NewSlogHandler(lc, nil)).Info("slog line "+run, "via", "slog")
	noErr(t, lc.Flush(ctx))
	if st := lc.Stats(); st.Sent != 3 || st.Failed != 0 || st.Dropped != 0 {
		t.Fatalf("stats %+v", st)
	}
	q := c.LogQuery(stream)
	recs := must(openobserve.WaitForLogs(ctx, q, run, 3, openobserve.WaitForLogOptions{Timeout: 30 * time.Second, Interval: 250 * time.Millisecond})).get(t)
	var exp openobserve.LogRecord
	for _, r := range recs {
		if strings.HasPrefix(r.Body, "json exporter") {
			exp = r
		}
	}
	if exp.Severity != openobserve.SeverityError || exp.SeverityText != "ERROR" || exp.TraceID != traceID || exp.SpanID != spanID ||
		exp.Resource["service.name"] != "go-it" || exp.Attributes["password"] != openobserve.Redacted || exp.TraceFlags != nil {
		t.Fatalf("json exporter record %+v", exp)
	}

	olc := must(c.NewLogClientWith(c.NewOTLPLogExporter(otlpStream), openobserve.LogClientOptions{Resource: map[string]openobserve.AttrValue{"service.name": "go-otlp"}})).get(t)
	olc.Send(ctx, openobserve.LogInput{Message: "otlp exporter " + run, Severity: openobserve.SeverityWarn, TraceID: traceID, SpanID: spanID, Attributes: map[string]openobserve.AttrValue{"n": 42}})
	noErr(t, olc.Close(ctx))
	orec := must(c.LogQuery(otlpStream).WaitForLog(ctx, "otlp exporter "+run, openobserve.WaitForLogOptions{Timeout: 30 * time.Second, Interval: 250 * time.Millisecond})).get(t)
	if orec.Severity != openobserve.SeverityWarn || orec.TraceID != traceID || orec.Resource["service.name"] != "go-otlp" || orec.Attributes["n"] != int64(42) {
		t.Fatalf("otlp record %+v", orec)
	}
}

func TestStreams(t *testing.T) {
	c, run := setup(t)
	ctx := ctxT(t)
	s := c.Streams()
	stream := "go_streams_" + run
	t.Cleanup(func() { _ = s.DeleteStream(context.Background(), stream, "") })

	noErr(t, s.CreateStream(ctx, stream, openobserve.CreateStreamOptions{
		Fields: []openobserve.SchemaField{{Name: "message", Type: "Utf8"}}, Settings: openobserve.StreamSettings{RetentionDays: 3},
	}))
	must(c.Logs().IngestJSON(ctx, stream, []map[string]any{{"message": "m", "run_id": run}})).get(t)
	info := must(s.GetStreamSchema(ctx, stream, openobserve.StreamTypeLogs)).get(t)
	if info.Name != stream || info.Settings.RetentionDays != 3 || !slices.ContainsFunc(info.Schema, func(f openobserve.SchemaField) bool { return f.Name == "run_id" }) {
		t.Fatalf("schema %+v", info)
	}
	days := 7
	noErr(t, s.UpdateStreamSettings(ctx, stream, openobserve.StreamSettingsUpdate{
		RetentionDays: &days, FullTextSearchKeys: []string{"message"}, PartitionKeys: []openobserve.PartitionKey{{Field: "run_id"}},
	}))
	info = must(s.GetStreamSchema(ctx, stream, "")).get(t)
	if info.Settings.RetentionDays != 7 || !slices.Contains(info.Settings.FullTextSearchKeys, "message") ||
		len(info.Settings.PartitionKeys) != 1 || info.Settings.PartitionKeys[0].Field != "run_id" || info.Settings.PartitionKeys[0].Disabled {
		t.Fatalf("after update %+v", info.Settings)
	}
	noErr(t, s.UpdateStreamSettings(ctx, stream, openobserve.StreamSettingsUpdate{FullTextSearchKeys: []string{}, PartitionKeys: []openobserve.PartitionKey{}}))
	info = must(s.GetStreamSchema(ctx, stream, "")).get(t)
	if len(info.Settings.FullTextSearchKeys) != 0 || len(info.Settings.PartitionKeys) != 1 || !info.Settings.PartitionKeys[0].Disabled {
		t.Fatalf("after removal %+v", info.Settings)
	}
	list := must(s.ListStreams(ctx, openobserve.ListStreamsOptions{Type: openobserve.StreamTypeLogs, Keyword: run})).get(t)
	if list.Total != 1 || len(list.Streams) != 1 || list.Streams[0].Name != stream {
		t.Fatalf("list %+v", list)
	}
	noErr(t, s.DeleteStream(ctx, stream, openobserve.StreamTypeLogs))
	if _, err := s.GetStreamSchema(ctx, stream, ""); !errors.Is(err, openobserve.ErrNotFound) {
		t.Fatal("deleted stream:", err)
	}
	if err := s.DeleteStream(ctx, stream, ""); !errors.Is(err, openobserve.ErrNotFound) {
		t.Fatal("second delete:", err)
	}
}

func TestTraces(t *testing.T) {
	c, run := setup(t)
	ctx := ctxT(t)
	service := "go-it-" + run
	h := must(c.InitTracing(ctx, openobserve.TracingConfig{ServiceName: service, ServiceVersion: "1.2.3", Environment: "test", DisableSetGlobal: true})).get(t)
	defer func() { _ = h.Shutdown(context.Background()) }()
	tracer := h.Tracer("integration")
	rctx, root := tracer.Start(ctx, "GET /checkout", trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attribute.String("run_id", run), attribute.Int("http.status_code", 200)))
	_, child := tracer.Start(rctx, "db.query", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attribute.String("db.system", "postgresql")))
	child.AddEvent("cache.miss", trace.WithAttributes(attribute.String("key", "u1")))
	child.SetStatus(codes.Error, "boom")
	time.Sleep(5 * time.Millisecond)
	child.End()
	root.End()
	traceID := root.SpanContext().TraceID().String()
	noErr(t, h.ForceFlush(ctx))

	tq := c.Traces()
	tr := must(tq.WaitForTrace(ctx, traceID, openobserve.WaitForTraceOptions{MinSpans: 2, Timeout: 60 * time.Second, Interval: 250 * time.Millisecond})).get(t)
	tree := tr.Tree()
	if len(tr.Spans) != 2 || len(tree) != 1 || tree[0].Span.Name != "GET /checkout" || len(tree[0].Children) != 1 {
		t.Fatalf("trace %+v", tr)
	}
	r, ch := tree[0].Span, tree[0].Children[0].Span
	if r.Kind != openobserve.SpanKindServer || r.ServiceName != service || r.Attributes["run_id"] != run || r.Attributes["http_status_code"] != "200" ||
		ch.Kind != openobserve.SpanKindClient || ch.Status.Code != openobserve.SpanStatusError || ch.Status.Message != "boom" ||
		len(ch.Events) != 1 || ch.Events[0].Name != "cache.miss" || ch.Events[0].Attributes["key"] != "u1" || ch.DurationMicros < 5000 {
		t.Fatalf("spans root=%+v child=%+v", r, ch)
	}
	if again := must(tq.GetTrace(ctx, traceID)).get(t); len(again.Spans) != 2 {
		t.Fatal("getTrace")
	}
	if missing := must(tq.GetTrace(ctx, "ffffffffffffffffffffffffffffffff")).get(t); missing != nil {
		t.Fatal("unknown trace must be nil")
	}
	sums := must(tq.SearchTraces(ctx, openobserve.TraceSearchQuery{Service: service, Limit: 10})).get(t)
	if len(sums) != 1 || sums[0].TraceID != traceID || sums[0].RootServiceName != service || sums[0].SpanCount != 2 || sums[0].RootName != "GET /checkout" {
		t.Fatalf("searchTraces %+v", sums)
	}
	tagged := must(tq.SearchTraces(ctx, openobserve.TraceSearchQuery{Tags: map[string]string{"db.system": "postgresql", "run_id": run}, MinDuration: time.Millisecond})).get(t)
	if len(tagged) != 0 {
		// run_id is on the root and db.system on the child: no single span matches both.
		t.Fatalf("tag filter matched %+v", tagged)
	}
	tagged = must(tq.SearchTraces(ctx, openobserve.TraceSearchQuery{Service: service, Operation: "db.query", Tags: map[string]string{"db.system": "postgresql"}})).get(t)
	if len(tagged) != 1 || tagged[0].TraceID != traceID {
		t.Fatalf("operation+tag filter %+v", tagged)
	}
	svcs := must(tq.ListServices(ctx)).get(t)
	if !slices.Contains(svcs, service) {
		t.Fatalf("services %v", svcs)
	}
	ops := must(tq.ListOperations(ctx, service)).get(t)
	if !slices.Equal(ops, []string{"GET /checkout", "db.query"}) {
		t.Fatalf("operations %v", ops)
	}
	tr2 := must(tq.TraceTimeRange(ctx, traceID)).get(t)
	if !tr2.Found || tr2.Stream != tq.Stream() || tr2.Start.IsZero() {
		t.Fatalf("time range %+v", tr2)
	}
	if _, err := tq.GetDependencies(ctx, openobserve.DependencyOptions{}); !errors.Is(err, openobserve.ErrUnsupported) {
		t.Fatal(err)
	}
}

func TestAdmin(t *testing.T) {
	c, run := setup(t)
	ctx := ctxT(t)
	a := c.Admin()
	cfg := c.Config()
	orgs := must(a.ListOrganizations(ctx)).get(t)
	if !slices.ContainsFunc(orgs, func(o openobserve.Organization) bool { return o.Identifier == cfg.Org }) {
		t.Fatalf("orgs %+v", orgs)
	}
	email := "go-it-" + run + "@stack.local"
	sa := must(a.CreateServiceAccount(ctx, email, openobserve.ServiceAccountOptions{FirstName: "Go", LastName: run})).get(t)
	t.Cleanup(func() { _ = a.DeleteServiceAccount(context.Background(), email) })
	if sa.Email != email || sa.Token.IsZero() || strings.Contains(fmt.Sprint(sa), sa.Token.Reveal()) {
		t.Fatalf("created %v", sa)
	}
	if _, err := a.CreateServiceAccount(ctx, email, openobserve.ServiceAccountOptions{}); !errors.Is(err, openobserve.ErrConflict) {
		t.Fatal("duplicate:", err)
	}
	client := func(user string, secret openobserve.Secret) *openobserve.Client {
		cl := must(openobserve.NewClient(openobserve.Config{CommonConfig: openobserve.CommonConfig{URL: cfg.URL}, User: user, Token: secret})).get(t)
		t.Cleanup(func() { _ = cl.Close() })
		return cl
	}
	old := client(email, sa.Token)
	must(old.Ping(ctx)).get(t)
	list := must(a.ListServiceAccounts(ctx)).get(t)
	if !slices.ContainsFunc(list, func(s openobserve.ServiceAccount) bool { return s.Email == email && s.LastName == run }) {
		t.Fatalf("list %+v", list)
	}
	rotated := must(a.RotateServiceAccountToken(ctx, email)).get(t)
	if rotated.Token.Reveal() == sa.Token.Reveal() {
		t.Fatal("token not rotated")
	}
	must(client(email, rotated.Token).Ping(ctx)).get(t)
	if _, err := old.Ping(ctx); !errors.Is(err, openobserve.ErrAuthentication) {
		t.Fatal("old token must stop working:", err)
	}

	pc := must(a.GetPasscode(ctx)).get(t)
	if pc.User != cfg.User || pc.Passcode.IsZero() {
		t.Fatalf("passcode %v", pc)
	}
	ingestOnly := client(pc.User, pc.Passcode)
	stream := "go_passcode_" + run
	t.Cleanup(func() { _ = c.Streams().DeleteStream(context.Background(), stream, "") })
	if res := must(ingestOnly.Logs().IngestJSON(ctx, stream, []map[string]any{{"message": "via passcode"}})).get(t); res.Successful != 1 {
		t.Fatalf("passcode ingest %+v", res)
	}
	if _, err := ingestOnly.Logs().Search(ctx, `SELECT * FROM "`+stream+`"`, openobserve.SQLSearchOptions{}); !errors.Is(err, openobserve.ErrAuthentication) {
		t.Fatal("passcode must not search:", err)
	}

	noErr(t, a.DeleteServiceAccount(ctx, email))
	if err := a.DeleteServiceAccount(ctx, email); !errors.Is(err, openobserve.ErrNotFound) {
		t.Fatal("second delete:", err)
	}
}
