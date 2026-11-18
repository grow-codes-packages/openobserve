package openobserve

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Client is the OpenObserve client. It is safe for concurrent use. Service APIs
// hang off it: Logs (ingest and SQL search), LogQuery (the LogQueryClient),
// Streams, Traces (the TraceQueryClient), Admin, NewLogClient and InitTracing.
type Client struct {
	cfg       Config
	transport *httpTransport
	auth      string

	mu         sync.Mutex
	logClients []*BatchingLogClient
	closeOnce  sync.Once
	closeErr   error
}

// NewClient validates cfg and returns a client.
func NewClient(cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()
	t, err := newHTTPTransport(cfg.CommonConfig)
	if err != nil {
		return nil, err
	}
	t.client.Transport = &captureTransport{base: t.client.Transport}
	c := &Client{cfg: cfg, transport: t, auth: basicAuth(cfg)}
	t.addSecret(nonEmpty(cfg.Password.Reveal(), cfg.Token.Reveal(), strings.TrimPrefix(c.auth, "Basic "))...)
	return c, nil
}

// NewClientFromEnv builds a client from the environment (see ConfigFromEnv).
func NewClientFromEnv() (*Client, error) {
	cfg, err := ConfigFromEnv()
	if err != nil {
		return nil, err
	}
	return NewClient(cfg)
}

// basicAuth builds the Authorization header value (OpenObserve accepts Basic only).
func basicAuth(cfg Config) string {
	if cfg.preEncoded() {
		return cfg.Token.Reveal()
	}
	secret := cfg.Password.Reveal()
	if secret == "" {
		secret = cfg.Token.Reveal()
	}
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(cfg.User+":"+secret))
}

func nonEmpty(values ...string) []string {
	out := values[:0]
	for _, v := range values {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

// Config returns the effective configuration (defaults applied).
func (c *Client) Config() Config { return c.cfg }

// orgPath returns "/api/<org>" + suffix.
func (c *Client) orgPath(suffix string) string { return "/api/" + c.cfg.Org + suffix }

// Health checks liveness without credentials (GET /healthz). It reports an
// unreachable or unhealthy service in the result; it fails only for CANCELLED
// and CONFIG.
func (c *Client) Health(ctx context.Context) (HealthStatus, error) {
	return c.transport.checkHealth(ctx, "health", &request{Method: http.MethodGet, Path: "/healthz"})
}

// Ping makes an authenticated round trip (GET /api/{org}/config) and returns
// its latency (AUTHENTICATION error on bad credentials).
func (c *Client) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	if _, err := c.do(ctx, &request{Method: http.MethodGet, Path: c.orgPath("/config"), Operation: "ping"}); err != nil {
		return 0, err
	}
	return time.Since(start), nil
}

// ServerVersion returns the server version, for example "v1.1.2" (authenticated;
// UNSUPPORTED when the server reports none).
func (c *Client) ServerVersion(ctx context.Context) (string, error) {
	var body struct {
		Version string `json:"version"`
	}
	if err := c.getJSON(ctx, "serverVersion", &request{Method: http.MethodGet, Path: c.orgPath("/config")}, &body); err != nil {
		return "", err
	}
	if body.Version == "" {
		return "", UnsupportedError("serverVersion", "the server reports no version")
	}
	return body.Version, nil
}

// Close flushes and closes every LogClient created by NewLogClient, then
// releases the transport. It is idempotent; later calls return the first result.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		clients := c.logClients
		c.logClients = nil
		c.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.TotalTimeout)
		defer cancel()
		for _, lc := range clients {
			if err := lc.Close(ctx); err != nil && c.closeErr == nil {
				c.closeErr = err
			}
		}
		c.transport.close()
	})
	return c.closeErr
}

// ---------------------------------------------------------------------------
// request helpers and OpenObserve error mapping

// do sends an authenticated request and maps OpenObserve error bodies.
func (c *Client) do(ctx context.Context, req *request) (*response, error) {
	if req.Header == nil {
		req.Header = http.Header{}
	}
	req.Header.Set("Authorization", c.auth)
	capture := &bodyCapture{}
	res, err := c.transport.do(context.WithValue(ctx, captureKey{}, capture), req)
	if err != nil {
		return nil, c.mapError(req.Operation, err, capture.get())
	}
	return res, nil
}

// getJSON runs do and decodes the JSON body into out (numbers as json.Number).
func (c *Client) getJSON(ctx context.Context, op string, req *request, out any) error {
	req.Operation = op
	res, err := c.do(ctx, req)
	if err != nil {
		return err
	}
	return decodeBody(op, res, out)
}

func decodeBody(op string, res *response, out any) error {
	dec := json.NewDecoder(bytes.NewReader(res.Body))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return &Error{Code: ErrorCodeUnavailable, Message: op + ": invalid JSON response", HTTPStatus: res.Status, RequestID: res.RequestID, Retryable: true}
	}
	return nil
}

// vendorError is the JSON error body of OpenObserve.
type vendorError struct {
	Code    json.Number `json:"code"`
	Message string      `json:"message"`
	TraceID string      `json:"trace_id"`
}

// Vendor codes seen on HTTP 400 responses.
const vendorStreamNotFound = "20002"

// mapError refines a status error with the response body: the vendor message
// (JSON or plain text), the search trace id as request id, and the codes
// OpenObserve reports with HTTP 400 (stream not found → NOT_FOUND, "already
// exists" → CONFLICT).
func (c *Client) mapError(op string, err error, body []byte) error {
	var e *Error
	if !errors.As(err, &e) || e.HTTPStatus == 0 {
		return err
	}
	text := strings.TrimSpace(string(body))
	var v vendorError
	isJSON := len(text) > 0 && json.Unmarshal(body, &v) == nil
	if isJSON {
		text = v.Message
		if text == "" {
			text = jsonErrorText(decodeAny(body))
		}
	} else if line, _, found := strings.Cut(text, "\n"); found {
		text = line
	}
	requestID := e.RequestID
	if requestID == "" {
		requestID = v.TraceID
	}
	out := StatusError(c.transport.redact(op), e.HTTPStatus, requestID, text, c.transport.knownSecrets())
	out.RetryAfter = e.RetryAfter
	lower := strings.ToLower(text)
	switch {
	case v.Code.String() == vendorStreamNotFound || (e.HTTPStatus == http.StatusBadRequest && strings.Contains(lower, "stream not found")):
		out.Code = ErrorCodeNotFound
	case e.HTTPStatus == http.StatusBadRequest && strings.Contains(lower, "already exists"):
		out.Code = ErrorCodeConflict
	}
	out.Retryable = out.Code.Retryable()
	return out
}

func decodeAny(body []byte) any {
	var doc any
	_ = json.Unmarshal(body, &doc)
	return doc
}

// captureKey carries a *bodyCapture in the request context.
type captureKey struct{}

// bodyCapture keeps the body of the last error response of one call.
type bodyCapture struct {
	mu   sync.Mutex
	body []byte
}

func (b *bodyCapture) set(body []byte) {
	b.mu.Lock()
	b.body = body
	b.mu.Unlock()
}

func (b *bodyCapture) get() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.body
}

// maxErrorBody bounds the error body kept for error mapping.
const maxErrorBody = 64 << 10

// captureTransport copies error response bodies into the call's bodyCapture so
// OpenObserve's vendor codes can be mapped after the core transport finished.
type captureTransport struct{ base http.RoundTripper }

// RoundTrip implements http.RoundTripper.
func (t *captureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(r)
	if err != nil || resp.StatusCode < 400 {
		return resp, err
	}
	capture, ok := r.Context().Value(captureKey{}).(*bodyCapture)
	if !ok {
		return resp, nil
	}
	data, rerr := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	_ = resp.Body.Close()
	if rerr != nil {
		return nil, rerr
	}
	capture.set(data)
	resp.Body = io.NopCloser(bytes.NewReader(data))
	return resp, nil
}
