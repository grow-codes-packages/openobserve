package openobserve

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func searchSQL(t *testing.T, r seenRequest) string {
	t.Helper()
	return jsonBody(t, r).(map[string]any)["query"].(map[string]any)["sql"].(string)
}

func TestLogQuerySearch(t *testing.T) {
	m := newMock(t, map[string][]string{"POST /api/default/_search": {"search.ok"}})
	c := newOOClient(t, m.URL, func(cfg *Config) { cfg.Stream = "My-Stream" })
	q := c.LogQuery("")
	if q.Stream() != "my_stream" || c.LogQuery("other").Stream() != "other" {
		t.Fatal("stream")
	}

	t.Run("full-text query", func(t *testing.T) {
		res, err := q.Search(t.Context(), "it's", SearchOptions{Limit: 5})
		if err != nil || len(res.Records) != 5 || res.NextCursor != "5" || res.Total != nil {
			t.Fatalf("%+v %v", res, err)
		}
		if sql := searchSQL(t, m.last()); sql != `SELECT * FROM "my_stream" WHERE match_all('it''s') ORDER BY _timestamp DESC` {
			t.Fatal(sql)
		}
		r := res.Records[2]
		if r.Body != "hello gamma ünïcødé" || r.Severity != SeverityWarn || r.SeverityText != "warn" || r.Attributes["n"] != int64(3) || r.Attributes["ok"] != false {
			t.Fatalf("%+v", r)
		}
		if _, ok := r.Attributes["message"]; ok {
			t.Fatal("message is the body, not an attribute")
		}
		if !r.Timestamp.Equal(time.UnixMicro(1795033200243779)) {
			t.Fatal(r.Timestamp)
		}
	})
	t.Run("raw SQL, empty query, ascending, cursor", func(t *testing.T) {
		if _, err := q.Search(t.Context(), "select * from x", SearchOptions{}); err != nil || searchSQL(t, m.last()) != "select * from x" {
			t.Fatal(err)
		}
		res, err := q.Search(t.Context(), "", SearchOptions{Order: OrderAsc, Cursor: "100"})
		if err != nil || res.NextCursor != "" {
			t.Fatalf("%+v %v", res, err)
		}
		body := jsonBody(t, m.last()).(map[string]any)["query"].(map[string]any)
		if body["sql"] != `SELECT * FROM "my_stream" ORDER BY _timestamp ASC` || body["from"].(json.Number).String() != "100" || body["size"].(json.Number).String() != "100" {
			t.Fatal(body)
		}
	})
	t.Run("errors", func(t *testing.T) {
		if _, err := q.Search(t.Context(), "x", SearchOptions{Cursor: "abc"}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		if _, err := q.Search(t.Context(), "x", SearchOptions{Start: time.Now(), End: time.Now().Add(-time.Hour)}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		bad := newMock(t, map[string][]string{"POST /api/default/_search": {"search.400_stream_not_found"}})
		if _, err := newOOClient(t, bad.URL, nil).LogQuery("").Search(t.Context(), "x", SearchOptions{}); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
	})
	t.Run("waitForLog", func(t *testing.T) {
		rec, err := q.WaitForLog(t.Context(), "needle", WaitForLogOptions{Timeout: time.Second, Interval: 10 * time.Millisecond, Predicate: func(r LogRecord) bool { return strings.Contains(r.Body, "alpha") }})
		if err != nil || rec.Body != "hello alpha needle6moiazbq" {
			t.Fatalf("%+v %v", rec, err)
		}
	})
}

func TestLogQueryStreamsAndLabels(t *testing.T) {
	m := newMock(t, map[string][]string{
		"GET /api/default/streams":                         {"listStreams.logs"},
		"GET /api/default/streams/" + fxStream + "/schema": {"getStreamSchema.ok"},
	})
	q := newOOClient(t, m.URL, nil).LogQuery(fxStream)
	names, err := q.ListStreams(t.Context())
	if err != nil || len(names) == 0 || names[0] != "passcode_6moiazbq" {
		t.Fatalf("%v %v", names, err)
	}
	if m.last().Query["type"][0] != "logs" {
		t.Fatal("type must be sent")
	}
	labels, err := q.ListLabels(t.Context())
	if err != nil || strings.Join(labels, ",") != "_timestamp,level,message,n,ok,run_id,src,user_id" {
		t.Fatalf("%v %v", labels, err)
	}
	bad := newMock(t, map[string][]string{"GET /api/default/streams": {"search.401"}, "GET /api/default/streams/x/schema": {"getStreamSchema.404"}})
	bq := newOOClient(t, bad.URL, nil).LogQuery("x")
	if _, err := bq.ListStreams(t.Context()); !errors.Is(err, ErrAuthentication) {
		t.Fatal(err)
	}
	if _, err := bq.ListLabels(t.Context()); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestRecordFromRow(t *testing.T) {
	r := recordFromRow(NormalizeAttributes(map[string]any{
		"_timestamp": json.Number("1795033200453175"), "body": "otlp", "message": "kept", "severity": "WARN", "severity_number": json.Number("13"),
		"trace_id": "038A1ED5AF73678E47809F854DABAAE9", "span_id": "00f067aa0ba902b7", "trace_flags": json.Number("1"),
		"service_name": "svc", "service_version": "1.0", "deployment_environment": "test", "host_name": "h1", "user_id": json.Number("42"),
		"_o2_ingest_ts": json.Number("1"),
	}))
	if r.Body != "otlp" || r.Severity != SeverityWarn || r.SeverityText != "WARN" || r.TraceID != "038a1ed5af73678e47809f854dabaae9" ||
		r.SpanID != "00f067aa0ba902b7" || r.TraceFlags == nil || *r.TraceFlags != 1 || r.Resource["service.name"] != "svc" ||
		r.Resource["host.name"] != "h1" || r.Attributes["user_id"] != int64(42) || r.Attributes["message"] != "kept" {
		t.Fatalf("%+v", r)
	}
	if _, ok := r.Attributes["_o2_ingest_ts"]; ok {
		t.Fatal("internal column")
	}
	r = recordFromRow(map[string]any{"level": "error", "message": "m"})
	if r.Severity != SeverityError || r.Body != "m" || !r.Timestamp.IsZero() {
		t.Fatalf("%+v", r)
	}
	r = recordFromRow(map[string]any{"severity_number": int64(9)})
	if r.Severity != SeverityInfo || r.SeverityText != "INFO" {
		t.Fatalf("%+v", r)
	}
	r = recordFromRow(map[string]any{"level": "weird"})
	if r.Severity != SeverityUnspecified || r.SeverityText != "weird" {
		t.Fatalf("%+v", r)
	}
}

func TestTail(t *testing.T) {
	var mu sync.Mutex
	polls := 0
	base := time.Now().Add(time.Second)
	row := func(i int) string {
		return fmt.Sprintf(`{"_timestamp":%d,"message":"tail %d"}`, base.Add(time.Duration(i)*time.Millisecond).UnixMicro(), i)
	}
	m := newMock(t, nil)
	m.handle("POST /api/default/_search", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		polls++
		n := polls
		mu.Unlock()
		switch n {
		case 1: // first poll: one row before the start floor, two new rows
			old := fmt.Sprintf(`{"_timestamp":%d,"message":"old"}`, base.Add(-time.Hour).UnixMicro())
			_, _ = w.Write([]byte(`{"hits":[` + old + `,` + row(0) + `,` + row(1) + `]}`))
		case 2: // overlap re-delivers row 1, adds row 2
			_, _ = w.Write([]byte(`{"hits":[` + row(1) + `,` + row(2) + `]}`))
		case 3:
			w.WriteHeader(http.StatusInternalServerError)
		default:
			_, _ = w.Write([]byte(`{"hits":[]}`))
		}
	})
	c := newOOClient(t, m.URL, func(cfg *Config) { cfg.Stream = "tails" })
	got := make(chan string, 10)
	h, err := c.LogQuery("").Tail(t.Context(), "needle", func(r LogRecord) {
		got <- r.Body
		if r.Body == "tail 0" {
			panic("callback panics are contained")
		}
	}, TailOptions{Start: base, Interval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	var bodies []string
	timeout := time.After(5 * time.Second)
	for len(bodies) < 3 {
		select {
		case b := <-got:
			bodies = append(bodies, b)
		case <-timeout:
			t.Fatalf("got %v", bodies)
		}
	}
	for {
		mu.Lock()
		n := polls
		mu.Unlock()
		if n >= 4 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.Stop()
	h.Stop()
	if strings.Join(bodies, ",") != "tail 0,tail 1,tail 2" {
		t.Fatal(bodies)
	}
	select {
	case b := <-got:
		t.Fatalf("duplicate %s", b)
	default:
	}
	if sql := searchSQL(t, m.requests()[0]); sql != `SELECT * FROM "tails" WHERE match_all('needle') ORDER BY _timestamp ASC` {
		t.Fatal(sql)
	}

	t.Run("validation and defaults", func(t *testing.T) {
		if _, err := c.LogQuery("").Tail(t.Context(), "", nil, TailOptions{}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		h, err := c.LogQuery("").Tail(t.Context(), "", func(LogRecord) {}, TailOptions{})
		if err != nil {
			t.Fatal(err)
		}
		h.Stop()
	})
	t.Run("pages through full polls", func(t *testing.T) {
		pm := newMock(t, nil)
		var calls int
		pm.handle("POST /api/default/_search", func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			if n == 1 {
				rows := make([]string, tailPageSize)
				for i := range rows {
					rows[i] = row(i)
				}
				_, _ = w.Write([]byte(`{"hits":[` + strings.Join(rows, ",") + `]}`))
				return
			}
			_, _ = w.Write([]byte(`{"hits":[]}`))
		})
		pc := newOOClient(t, pm.URL, nil)
		count := make(chan struct{}, tailPageSize+1)
		h, _ := pc.LogQuery("").Tail(t.Context(), "", func(LogRecord) { count <- struct{}{} }, TailOptions{Start: base, Interval: time.Hour})
		for i := 0; i < tailPageSize; i++ {
			select {
			case <-count:
			case <-time.After(5 * time.Second):
				t.Fatalf("only %d records", i)
			}
		}
		deadline := time.Now().Add(5 * time.Second)
		for len(pm.requests()) < 2 && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		h.Stop()
		reqs := pm.requests()
		if len(reqs) < 2 || jsonBody(t, reqs[1]).(map[string]any)["query"].(map[string]any)["from"].(json.Number).String() != fmt.Sprint(tailPageSize) {
			t.Fatal("second page not requested")
		}
	})
}
