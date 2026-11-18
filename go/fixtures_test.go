package openobserve

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// fixture is a recorded request/response pair from the real OpenObserve v1.1.2
// (testdata/fixtures, sanitized).
type fixture struct {
	Request struct {
		Method string          `json:"method"`
		Path   string          `json:"path"`
		Body   json.RawMessage `json:"body"`
	} `json:"request"`
	Response struct {
		Status  int               `json:"status"`
		Headers map[string]string `json:"headers"`
		Body    json.RawMessage   `json:"body"`
	} `json:"response"`
}

func loadFixture(t *testing.T, name string) fixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "fixtures", name+".json"))
	if err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	var f fixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	return f
}

// writeFixture replays a fixture response: a JSON string body is sent as raw
// text, anything else as JSON.
func writeFixture(t *testing.T, w http.ResponseWriter, name string) {
	t.Helper()
	f := loadFixture(t, name)
	for k, v := range f.Response.Headers {
		w.Header().Set(k, v)
	}
	body := []byte(f.Response.Body)
	var s string
	if json.Unmarshal(f.Response.Body, &s) == nil {
		body = []byte(s)
	}
	if bytes.Equal(body, []byte("null")) {
		body = nil
	}
	w.WriteHeader(f.Response.Status)
	_, _ = w.Write(body)
}

// seenRequest is a request received by the mock server (body gunzipped).
type seenRequest struct {
	Method string
	Path   string
	Query  map[string][]string
	Header http.Header
	Body   []byte
	Gzip   bool
}

// mock is a mock OpenObserve that answers "METHOD /path" with fixture
// responses. A route may hold several fixtures that are served in turn (the
// last one repeats).
type mock struct {
	*httptest.Server
	t      *testing.T
	mu     sync.Mutex
	routes map[string][]string
	hits   map[string]int
	seen   []seenRequest
	custom map[string]http.HandlerFunc
}

func newMock(t *testing.T, routes map[string][]string) *mock {
	t.Helper()
	m := &mock{t: t, routes: routes, hits: map[string]int{}, custom: map[string]http.HandlerFunc{}}
	m.Server = httptest.NewServer(http.HandlerFunc(m.serve))
	t.Cleanup(m.Close)
	return m
}

// handle installs a custom handler for a route.
func (m *mock) handle(route string, h http.HandlerFunc) {
	m.mu.Lock()
	m.custom[route] = h
	m.mu.Unlock()
}

func (m *mock) serve(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	sr := seenRequest{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query(), Header: r.Header.Clone(), Body: raw}
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		if err == nil {
			sr.Body, _ = io.ReadAll(zr)
			sr.Gzip = true
		}
	}
	key := r.Method + " " + r.URL.Path
	m.mu.Lock()
	m.seen = append(m.seen, sr)
	custom := m.custom[key]
	names := m.routes[key]
	n := m.hits[key]
	m.hits[key] = n + 1
	m.mu.Unlock()
	r.Body = io.NopCloser(bytes.NewReader(sr.Body))
	switch {
	case custom != nil:
		custom(w, r)
	case len(names) > 0:
		writeFixture(m.t, w, names[min(n, len(names)-1)])
	default:
		m.t.Errorf("unexpected request %s", key)
		w.WriteHeader(599)
	}
}

// requests returns the requests seen so far.
func (m *mock) requests() []seenRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]seenRequest(nil), m.seen...)
}

// last returns the last request.
func (m *mock) last() seenRequest {
	reqs := m.requests()
	if len(reqs) == 0 {
		m.t.Fatal("no request seen")
	}
	return reqs[len(reqs)-1]
}

const (
	testUser     = "admin@stack.local"
	testPassword = "pw-SECRET-9876"
)

// newOOClient builds a client for the mock with Basic user/password auth and
// no retries; mutate adjusts the config.
func newOOClient(t *testing.T, url string, mutate func(*Config)) *Client {
	t.Helper()
	cfg := Config{CommonConfig: CommonConfig{URL: url, Retry: fastRetry(0)}, User: testUser, Password: NewSecret(testPassword)}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// jsonBody decodes a request body.
func jsonBody(t *testing.T, r seenRequest) any {
	t.Helper()
	var v any
	dec := json.NewDecoder(bytes.NewReader(r.Body))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("request body is not JSON: %v (%q)", err, r.Body)
	}
	return v
}
