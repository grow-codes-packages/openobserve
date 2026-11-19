package openobserve

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"
)

func sampleRecord() LogRecord {
	flags := uint8(1)
	return LogRecord{
		Timestamp: time.UnixMicro(1795033200453175), ObservedTimestamp: time.UnixMicro(1795033200453176),
		Severity: SeverityWarn, Body: "hello", Attributes: map[string]AttrValue{"run": "r1", "n": int64(2), "nested": map[string]AttrValue{"a": true}, "list": []AttrValue{"x", 1.5}},
		Resource: map[string]AttrValue{"service.name": "svc", "deployment.environment": "test"},
		TraceID:  "038a1ed5af73678e47809f854dabaae9", SpanID: "00f067aa0ba902b7", TraceFlags: &flags,
	}
}

func TestJSONLogExporter(t *testing.T) {
	m := newMock(t, map[string][]string{
		"POST /api/default/sdk_logs_6moiazbq/_json":         {"ingestJson.ok"},
		"POST /api/default/sdk_logs_6moiazbq_partial/_json": {"ingestJson.200_partial_failure"},
		"POST /api/default/default/_json":                   {"ingestJson.401"},
	})
	c := newOOClient(t, m.URL, nil)

	t.Run("row mapping and gzip", func(t *testing.T) {
		e := c.NewJSONLogExporter(fxStream)
		res, err := e.Export(t.Context(), []LogRecord{sampleRecord()}, ExportOptions{Gzip: true})
		if err != nil || res.Accepted != 3 || res.Rejected != 0 || res.Error != nil {
			t.Fatalf("%+v %v", res, err)
		}
		r := m.last()
		if !r.Gzip || r.Header.Get("Content-Encoding") != "gzip" {
			t.Fatal("gzip")
		}
		row := jsonBody(t, r).([]any)[0].(map[string]any)
		checks := map[string]string{
			"_timestamp": "1795033200453175", "body": "hello", "severity": "WARN", "severity_number": "13",
			"trace_id": "038a1ed5af73678e47809f854dabaae9", "span_id": "00f067aa0ba902b7", "trace_flags": "1",
			"service_name": "svc", "deployment_environment": "test", "run": "r1", "n": "2",
		}
		for k, want := range checks {
			if got := toString(row[k]); got != want {
				t.Errorf("%s = %q, want %q", k, got, want)
			}
		}
		if e.Shutdown(t.Context()) != nil {
			t.Fatal("shutdown")
		}
	})
	t.Run("partial failure", func(t *testing.T) {
		res, err := c.NewJSONLogExporter("sdk_logs_6moiazbq_partial").Export(t.Context(), []LogRecord{sampleRecord(), {Body: "x"}}, ExportOptions{})
		if err != nil || res.Accepted != 1 || res.Rejected != 1 || !errors.Is(res.Error, ErrValidation) || !strings.Contains(res.Error.Message, "Too old") {
			t.Fatalf("%+v %v", res, err)
		}
		if m.last().Gzip {
			t.Fatal("no gzip when disabled")
		}
	})
	t.Run("request failure and encoding failure", func(t *testing.T) {
		if _, err := c.NewJSONLogExporter("").Export(t.Context(), []LogRecord{{Body: "x"}}, ExportOptions{}); !errors.Is(err, ErrAuthentication) {
			t.Fatal(err)
		}
		bad := LogRecord{Attributes: map[string]AttrValue{"f": math.Inf(1)}}
		if _, err := c.NewJSONLogExporter("s").Export(t.Context(), []LogRecord{bad}, ExportOptions{}); !errors.Is(err, ErrValidation) {
			t.Fatal(err)
		}
		dec := newMock(t, nil)
		dec.handle("POST /api/default/s/_json", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("x")) })
		if _, err := newOOClient(t, dec.URL, nil).NewJSONLogExporter("s").Export(t.Context(), []LogRecord{{}}, ExportOptions{}); !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
		partial := newMock(t, nil)
		partial.handle("POST /api/default/s/_json", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"code":200,"status":[{"name":"s","successful":0,"failed":1}]}`))
		})
		res, err := newOOClient(t, partial.URL, nil).NewJSONLogExporter("s").Export(t.Context(), []LogRecord{{}}, ExportOptions{})
		if err != nil || !strings.Contains(res.Error.Message, "records rejected") {
			t.Fatal(res, err)
		}
	})
	t.Run("severity text default", func(t *testing.T) {
		row := recordRow(LogRecord{Severity: SeverityError})
		if row["severity"] != "ERROR" || row["severity_number"] != 17 {
			t.Fatal(row)
		}
		if _, ok := row["trace_id"]; ok {
			t.Fatal("no trace id")
		}
	})
}

func TestOTLPLogExporter(t *testing.T) {
	m := newMock(t, map[string][]string{"POST /api/default/v1/logs": {"otlpLogs.json"}})
	c := newOOClient(t, m.URL, nil)
	e := c.NewOTLPLogExporter("sdk_otlp_6moiazbq")
	other := sampleRecord()
	other.Resource = map[string]AttrValue{"service.name": "other"}
	other.TraceID, other.SpanID, other.TraceFlags, other.SeverityText = "", "", nil, "custom"
	res, err := e.Export(t.Context(), []LogRecord{sampleRecord(), sampleRecord(), other}, ExportOptions{Gzip: true})
	if err != nil || res.Accepted != 3 || res.Rejected != 0 {
		t.Fatalf("%+v %v", res, err)
	}
	r := m.last()
	if r.Header.Get("Stream-Name") != "sdk_otlp_6moiazbq" || r.Header.Get("Content-Type") != "application/json" || !r.Gzip {
		t.Fatal(r.Header)
	}
	body := jsonBody(t, r).(map[string]any)
	rls := body["resourceLogs"].([]any)
	if len(rls) != 2 {
		t.Fatalf("grouped by resource: %d", len(rls))
	}
	first := rls[0].(map[string]any)
	logs := first["scopeLogs"].([]any)[0].(map[string]any)["logRecords"].([]any)
	lr := logs[0].(map[string]any)
	if len(logs) != 2 || lr["timeUnixNano"] != "1795033200453175000" || lr["severityText"] != "WARN" || lr["traceId"] != "038a1ed5af73678e47809f854dabaae9" ||
		lr["spanId"] != "00f067aa0ba902b7" || toString(lr["flags"]) != "1" || lr["observedTimeUnixNano"] == nil {
		t.Fatalf("%v", lr)
	}
	raw, _ := json.Marshal(lr["attributes"])
	for _, want := range []string{`"intValue":"2"`, `"kvlistValue"`, `"arrayValue"`, `"doubleValue":1.5`, `"boolValue":true`, `"stringValue":"r1"`} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("attributes %s missing %s", raw, want)
		}
	}
	if e.Shutdown(t.Context()) != nil {
		t.Fatal("shutdown")
	}

	t.Run("partial success", func(t *testing.T) {
		pm := newMock(t, nil)
		pm.handle("POST /api/default/v1/logs", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"partialSuccess":{"rejectedLogRecords":"5","errorMessage":"too old"}}`))
		})
		res, err := newOOClient(t, pm.URL, nil).NewOTLPLogExporter("").Export(t.Context(), []LogRecord{{}, {}}, ExportOptions{})
		if err != nil || res.Accepted != 0 || res.Rejected != 2 || !strings.Contains(res.Error.Message, "too old") {
			t.Fatalf("%+v %v", res, err)
		}
		if pm.last().Header.Get("Stream-Name") != "default" {
			t.Fatal("default stream")
		}
	})
	t.Run("errors", func(t *testing.T) {
		bm := newMock(t, map[string][]string{"POST /api/default/v1/logs": {"otlpLogs.401"}})
		if _, err := newOOClient(t, bm.URL, nil).NewOTLPLogExporter("s").Export(t.Context(), []LogRecord{{}}, ExportOptions{}); !errors.Is(err, ErrAuthentication) {
			t.Fatal(err)
		}
		bad := LogRecord{Attributes: map[string]AttrValue{"f": map[string]AttrValue{"x": math.NaN()}}}
		if _, err := c.NewOTLPLogExporter("s").Export(t.Context(), []LogRecord{bad}, ExportOptions{}); err != nil {
			t.Fatal("NaN becomes a string value", err)
		}
	})
	t.Run("any values", func(t *testing.T) {
		if v := otlpAnyValue(nil); v["stringValue"] != "" {
			t.Fatal(v)
		}
		if v := otlpAnyValue(math.Inf(1)); v["stringValue"] != "+Inf" {
			t.Fatal(v)
		}
		if isHex("zz", 2) || isHex("ab", 3) || !isHex("ab", 2) {
			t.Fatal("isHex")
		}
	})
}

func TestNewLogClient(t *testing.T) {
	m := newMock(t, map[string][]string{"POST /api/default/sdk_logs_6moiazbq/_json": {"ingestJson.ok"}})
	c := newOOClient(t, m.URL, nil)
	lc, err := c.NewLogClient(fxStream, LogClientOptions{Resource: map[string]AttrValue{"service.name": "svc"}})
	if err != nil {
		t.Fatal(err)
	}
	lc.Logger("test", nil).Info(t.Context(), "hello", "password", "hunter2")
	if err := lc.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	row := jsonBody(t, m.last()).([]any)[0].(map[string]any)
	if row["body"] != "hello" || row["service_name"] != "svc" || row["password"] != Redacted {
		t.Fatal(row)
	}
	if _, err := c.NewLogClientWith(nil, LogClientOptions{}); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	if _, err := c.NewLogClient("s", LogClientOptions{Batch: BatchOptions{MaxQueueSize: -1}}); err == nil {
		t.Fatal("invalid batch options")
	}
}
