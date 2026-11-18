package openobserve

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	fxTraces  = "sdk_traces_6moiazbq"
	fxTraceID = "038a1ed5af73678e47809f854dabaae9"
)

func TestGetTrace(t *testing.T) {
	m := newMock(t, map[string][]string{
		"GET /api/default/" + fxTraces + "/traces/" + fxTraceID + "/details":                {"getTrace.details"},
		"GET /api/default/" + fxTraces + "/traces/ffffffffffffffffffffffffffffffff/details": {"getTrace.not_found"},
		"GET /api/default/missing/traces/" + fxTraceID + "/details":                         {"getTrace.400_stream_not_found"},
	})
	c := newOOClient(t, m.URL, func(cfg *Config) { cfg.TracesStream = fxTraces })
	tr, err := c.Traces().GetTrace(t.Context(), strings.ToUpper(fxTraceID))
	if err != nil || tr == nil || tr.TraceID != fxTraceID || len(tr.Spans) != 3 {
		t.Fatalf("%+v %v", tr, err)
	}
	q := m.last().Query
	if q["start_time"] == nil || q["end_time"] == nil {
		t.Fatal("window must be sent", q)
	}
	s, _ := json.Number(q["start_time"][0]).Int64()
	e, _ := json.Number(q["end_time"][0]).Int64()
	if e-s != DefaultGetTraceWindow.Microseconds() {
		t.Fatal("7 day window")
	}
	root, db := tr.Spans[0], tr.Spans[1]
	if root.Name != "GET /checkout" || root.Kind != SpanKindServer || root.ParentSpanID != "" || root.Status.Code != SpanStatusOK ||
		root.ServiceName != "sdk-a-6moiazbq" || root.StartMicros != 1795033200603151 || root.DurationMicros != 50000 ||
		root.Attributes["http_method"] != "GET" || root.Attributes["service_service_version"] != "1.2.3" {
		t.Fatalf("%+v", root)
	}
	for _, internal := range []string{"_timestamp", "flags", "events", "trace_id", "duration", "span_status"} {
		if _, ok := root.Attributes[internal]; ok {
			t.Errorf("internal column %s", internal)
		}
	}
	if db.Kind != SpanKindClient || db.ParentSpanID != "b7ad6b7169203331" || db.Status != (SpanStatus{Code: SpanStatusError, Message: "boom"}) ||
		len(db.Events) != 1 || db.Events[0].Name != "cache.miss" || db.Events[0].TimeMicros != 1795033200623151 || db.Events[0].Attributes["key"] != "u1" ||
		len(db.Links) != 1 || db.Links[0].SpanID != "00f067aa0ba902b8" || db.Links[0].Attributes["kind"] != "follows" {
		t.Fatalf("%+v", db)
	}
	for k := range db.Attributes {
		if strings.HasPrefix(k, "o2_db_") || strings.HasPrefix(k, "infer_service_") {
			t.Errorf("derived column %s", k)
		}
	}
	if tree := tr.Tree(); len(tree) != 1 || len(tree[0].Children) != 1 {
		t.Fatalf("tree %+v", tree)
	}

	t.Run("not found and errors", func(t *testing.T) {
		if tr, err := c.Traces().GetTrace(t.Context(), "ffffffffffffffffffffffffffffffff"); err != nil || tr != nil {
			t.Fatal(tr, err)
		}
		if _, err := c.TracesFor("missing").GetTrace(t.Context(), fxTraceID); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if _, err := c.Traces().GetTrace(t.Context(), "not-hex"); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
	})
	t.Run("pagination", func(t *testing.T) {
		pm := newMock(t, nil)
		var mu sync.Mutex
		var queries []map[string][]string
		pm.handle("GET /api/default/default/traces/"+fxTraceID+"/details", func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			queries = append(queries, r.URL.Query())
			n := len(queries)
			mu.Unlock()
			if n == 1 {
				writeFixture(t, w, "getTrace.page1")
				return
			}
			_, _ = w.Write([]byte(`{"hits":[{"trace_id":"` + fxTraceID + `","span_id":"00f067aa0ba902b7","operation_name":"two","start_time":1795033200613151382,"end_time":1795033200643151382,"span_kind":"9","events":"bad json"}],"has_more":false,"next_after":null}`))
		})
		tr, err := newOOClient(t, pm.URL, nil).Traces().GetTrace(t.Context(), fxTraceID)
		if err != nil || len(tr.Spans) != 2 || tr.Spans[1].Kind != SpanKindUnspecified || len(tr.Spans[1].Events) != 0 {
			t.Fatalf("%+v %v", tr, err)
		}
		if queries[1]["after_start_time"][0] != "1795033200603151382" || queries[1]["after_span_id"][0] != "b7ad6b7169203331" {
			t.Fatal(queries[1])
		}
	})
	t.Run("waitForTrace", func(t *testing.T) {
		tr, err := c.Traces().WaitForTrace(t.Context(), fxTraceID, WaitForTraceOptions{MinSpans: 3, Timeout: time.Second, Interval: 10 * time.Millisecond})
		if err != nil || len(tr.Spans) != 3 {
			t.Fatal(err)
		}
	})
}

func TestSearchTraces(t *testing.T) {
	m := newMock(t, map[string][]string{"GET /api/default/" + fxTraces + "/traces/latest": {"searchTraces.latest"}})
	tr := newOOClient(t, m.URL, nil).TracesFor(fxTraces)
	if tr.Stream() != fxTraces {
		t.Fatal("stream")
	}
	sums, err := tr.SearchTraces(t.Context(), TraceSearchQuery{})
	if err != nil || len(sums) != 2 {
		t.Fatalf("%+v %v", sums, err)
	}
	want := TraceSummary{TraceID: fxTraceID, RootServiceName: "sdk-a-6moiazbq", RootName: "GET /checkout", StartMicros: 1795033200603151, DurationMicros: 50000, SpanCount: 3}
	if sums[1] != want {
		t.Fatalf("%+v", sums[1])
	}
	q := m.last().Query
	s, _ := json.Number(q["start_time"][0]).Int64()
	e, _ := json.Number(q["end_time"][0]).Int64()
	if e-s != DefaultTraceSearchWindow.Microseconds() || q["size"][0] != "20" || q["from"][0] != "0" || q["filter"] != nil {
		t.Fatal(q)
	}
	_, err = tr.SearchTraces(t.Context(), TraceSearchQuery{
		Service: "svc", Operation: "it's", MinDuration: 40 * time.Millisecond, MaxDuration: time.Second,
		Tags: map[string]string{"db.system": "postgresql", "HTTP-Method": "GET", "": "skip"}, Limit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	q = m.last().Query
	wantFilter := `service_name = 'svc' AND operation_name = 'it''s' AND duration >= 40000 AND duration <= 1000000 AND "http_method" = 'GET' AND "db_system" = 'postgresql'`
	if q["filter"][0] != wantFilter || q["size"][0] != "5" {
		t.Fatalf("filter %q", q["filter"][0])
	}
	if _, err := tr.SearchTraces(t.Context(), TraceSearchQuery{Start: time.Now(), End: time.Now().Add(-time.Hour)}); !errors.Is(err, ErrValidation) {
		t.Fatal(err)
	}
	bad := newMock(t, map[string][]string{"GET /api/default/default/traces/latest": {"search.401"}})
	if _, err := newOOClient(t, bad.URL, nil).Traces().SearchTraces(t.Context(), TraceSearchQuery{}); !errors.Is(err, ErrAuthentication) {
		t.Fatal(err)
	}
}

func TestServicesOperationsDependencies(t *testing.T) {
	m := newMock(t, map[string][]string{"POST /api/default/_search": {"listServices.ok", "listOperations.ok"}})
	tr := newOOClient(t, m.URL, nil).TracesFor(fxTraces)
	svcs, err := tr.ListServices(t.Context())
	if err != nil || strings.Join(svcs, ",") != "sdk-a-6moiazbq,sdk-b-6moiazbq" {
		t.Fatal(svcs, err)
	}
	r := m.last()
	if r.Query["type"][0] != "traces" || searchSQL(t, r) != `SELECT DISTINCT service_name FROM "sdk_traces_6moiazbq" ORDER BY service_name` {
		t.Fatal(r.Query, searchSQL(t, r))
	}
	ops, err := tr.ListOperations(t.Context(), "sdk-a-6moiazbq")
	if err != nil || len(ops) == 0 {
		t.Fatal(ops, err)
	}
	if sql := searchSQL(t, m.last()); sql != `SELECT DISTINCT operation_name FROM "sdk_traces_6moiazbq" WHERE service_name = 'sdk-a-6moiazbq' ORDER BY operation_name` {
		t.Fatal(sql)
	}
	if _, err := tr.ListOperations(t.Context(), ""); !errors.Is(err, ErrValidation) {
		t.Fatal(err)
	}
	if _, err := tr.GetDependencies(t.Context(), DependencyOptions{}); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	bad := newMock(t, map[string][]string{"POST /api/default/_search": {"search.401"}})
	if _, err := newOOClient(t, bad.URL, nil).Traces().ListServices(t.Context()); !errors.Is(err, ErrAuthentication) {
		t.Fatal(err)
	}
}

func TestTraceTimeRange(t *testing.T) {
	m := newMock(t, map[string][]string{"GET /api/default/traces/time_range": {"traceTimeRange.ok", "traceTimeRange.not_found", "search.401"}})
	tr := newOOClient(t, m.URL, nil).Traces()
	r, err := tr.TraceTimeRange(t.Context(), fxTraceID)
	if err != nil || !r.Found || r.Stream != fxTraces || !r.Start.Equal(time.UnixMicro(1795033200603151)) || !r.End.Equal(time.UnixMicro(1795033200653152)) {
		t.Fatalf("%+v %v", r, err)
	}
	if m.last().Query["trace_id"][0] != fxTraceID {
		t.Fatal("trace id")
	}
	r, err = tr.TraceTimeRange(t.Context(), "ffffffffffffffffffffffffffffffff")
	if err != nil || r.Found || r.TraceID != "ffffffffffffffffffffffffffffffff" {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err := tr.TraceTimeRange(t.Context(), fxTraceID); !errors.Is(err, ErrAuthentication) {
		t.Fatal(err)
	}
	if _, err := tr.TraceTimeRange(t.Context(), "zz"); !errors.Is(err, ErrValidation) {
		t.Fatal(err)
	}
}

func TestInitTracing(t *testing.T) {
	var mu sync.Mutex
	var got []seenRequest
	m := newMock(t, nil)
	m.handle("POST /proxy/api/myorg/v1/traces", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got = append(got, seenRequest{Header: r.Header.Clone()})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	})
	cfg := Config{CommonConfig: CommonConfig{URL: m.URL + "/proxy/"}, Org: "myorg", User: testUser, Password: NewSecret(testPassword), TracesStream: "My-Traces"}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	h, err := c.InitTracing(t.Context(), TracingConfig{ServiceName: "svc", DisableSetGlobal: true, Compression: CompressionNone})
	if err != nil {
		t.Fatal(err)
	}
	_, span := h.Tracer("test").Start(t.Context(), "op")
	span.End()
	if err := h.ForceFlush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := h.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("no export")
	}
	hdr := got[0].Header
	if hdr.Get("Authorization") != basicAuth(c.cfg) || hdr.Get("Stream-Name") != "my_traces" || hdr.Get("Content-Type") != "application/x-protobuf" {
		t.Fatal(hdr)
	}
	if strings.Contains(h.Config().String(), testPassword) {
		t.Fatal("tracing config leaks credentials")
	}
}

func TestTracingConfigFor(t *testing.T) {
	cfg := Config{CommonConfig: CommonConfig{URL: "https://oo.example:5080"}, User: "u", Token: NewSecret("t")}
	tc, err := tracingConfigFor(cfg, TracingConfig{Protocol: ProtocolGRPC, Headers: map[string]string{"stream-name": "mine"}})
	if err != nil || tc.Endpoint != "https://oo.example:5081" || tc.Headers["organization"] != "default" || tc.Headers["stream-name"] != "mine" || tc.Headers["Authorization"] == "" {
		t.Fatalf("%+v %v", tc, err)
	}
	ca := TLSConfig{CACertPEM: "x"}
	tc, _ = tracingConfigFor(Config{CommonConfig: CommonConfig{TLS: ca}, User: "u", Token: NewSecret("t")}, TracingConfig{Endpoint: "http://collector:4318/v1/traces", Headers: map[string]string{"authorization": "Basic own"}})
	if tc.Endpoint != "http://collector:4318/v1/traces" || tc.Headers["authorization"] != "Basic own" || tc.Headers["Authorization"] != "" || tc.TLS != ca {
		t.Fatalf("%+v", tc)
	}
	if _, err := tracingConfigFor(cfg, TracingConfig{Protocol: ProtocolNative}); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	if _, err := tracingConfigFor(Config{}, TracingConfig{}); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	if _, err := InitTracing(t.Context(), Config{}, TracingConfig{ServiceName: "x"}); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	if _, err := TraceExporterFactory(cfg)(t.Context(), TracingConfig{Protocol: ProtocolNative}); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	exp, err := TraceExporterFactory(cfg)(t.Context(), TracingConfig{Protocol: ProtocolGRPC}.WithDefaults())
	if err != nil {
		t.Fatal(err)
	}
	_ = exp.Shutdown(t.Context())
}
