package openobserve

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	t.Run("core.config.env-precedence", func(t *testing.T) {
		env := map[string]string{
			"OPENOBSERVE_URL": "http://vendor:5080", "OPENOBSERVE_ORG": "vendor-org", "OPENOBSERVE_USER": "v@x.io",
			"OPENOBSERVE_PASSWORD": "vendor-pw", "OPENOBSERVE_STREAM": "vlogs", "OPENOBSERVE_TRACES_STREAM": "vtraces",
		}
		cfg, err := ConfigFromEnvWith(Config{}, env)
		if err != nil || cfg.URL != "http://vendor:5080" || cfg.Org != "vendor-org" || cfg.User != "v@x.io" ||
			cfg.Password.Reveal() != "vendor-pw" || cfg.Stream != "vlogs" || cfg.TracesStream != "vtraces" {
			t.Fatalf("vendor env: %v %s", err, cfg)
		}
		env[EnvPrefix+"URL"] = "http://grow:1"
		env[EnvPrefix+"ORG"] = "grow-org"
		env[EnvPrefix+"STREAM"] = "glogs"
		cfg, _ = ConfigFromEnvWith(Config{}, env)
		if cfg.URL != "http://grow:1" || cfg.Org != "grow-org" || cfg.Stream != "glogs" {
			t.Fatalf("GROW_* must beat vendor: %s", cfg)
		}
		cfg, _ = ConfigFromEnvWith(Config{CommonConfig: CommonConfig{URL: "https://explicit"}, Org: "e", Password: NewSecret("explicit")}, env)
		if cfg.URL != "https://explicit" || cfg.Org != "e" || cfg.Password.Reveal() != "explicit" {
			t.Fatal("explicit must win")
		}
		cfg, err = ConfigFromEnvWith(Config{}, map[string]string{"OPENOBSERVE_USER": "u", "OPENOBSERVE_TOKEN": "tok"})
		if err != nil || cfg.URL != DefaultURL || cfg.Org != DefaultOrg || cfg.Stream != DefaultStream ||
			cfg.TracesStream != DefaultStream || cfg.Token.Reveal() != "tok" || cfg.RequestTimeout != DefaultRequestTimeout {
			t.Fatalf("defaults: %v %s", err, cfg)
		}
		_, err = ConfigFromEnvWith(Config{}, map[string]string{EnvPrefix + "TOTAL_TIMEOUT": "forever-xyz"})
		if !errors.Is(err, ErrConfig) || !strings.Contains(err.Error(), EnvPrefix+"TOTAL_TIMEOUT") || strings.Contains(err.Error(), "forever-xyz") {
			t.Fatalf("invalid value: %v", err)
		}
		t.Setenv(EnvPrefix+"URL", "http://process-env:9")
		t.Setenv(EnvPrefix+"USER", "u")
		t.Setenv(EnvPrefix+"PASSWORD", "p")
		if cfg, err := ConfigFromEnv(); err != nil || cfg.URL != "http://process-env:9" {
			t.Fatalf("process env: %v %s", err, cfg)
		}
	})

	t.Run("validate", func(t *testing.T) {
		base := Config{User: "u", Password: NewSecret("p")}
		cases := map[string]Config{
			"no credentials":     {User: "u"},
			"both credentials":   {User: "u", Password: NewSecret("p"), Token: NewSecret("t")},
			"token without user": {Token: NewSecret("raw")},
			"pw without user":    {Password: NewSecret("p")},
			"bad org":            {User: "u", Password: NewSecret("p"), Org: "a/b"},
			"blank stream":       {User: "u", Password: NewSecret("p"), Stream: "  "},
			"bad url":            {User: "u", Password: NewSecret("p"), CommonConfig: CommonConfig{URL: "ftp://x"}},
		}
		for name, cfg := range cases {
			if err := cfg.Validate(); !errors.Is(err, ErrConfig) {
				t.Errorf("%s: %v", name, err)
			}
			if _, err := NewClient(cfg); !errors.Is(err, ErrConfig) {
				t.Errorf("%s NewClient: %v", name, err)
			}
		}
		if err := base.Validate(); err != nil {
			t.Fatal(err)
		}
		if err := (Config{Token: NewSecret("Basic abc")}).Validate(); err != nil {
			t.Fatalf("pre-encoded token needs no user: %v", err)
		}
	})

	t.Run("core.redaction.config", func(t *testing.T) {
		cfg := Config{User: "u@x.io", Password: NewSecret("pw-SECRET"), Token: NewSecret("tok-SECRET")}
		for _, s := range []string{cfg.String(), cfg.GoString(), fmt.Sprintf("%v %+v %#v", cfg, cfg, cfg)} {
			if strings.Contains(s, "SECRET") || !strings.Contains(s, Redacted) || !strings.Contains(s, "u@x.io") {
				t.Fatalf("leak: %s", s)
			}
		}
	})
}

func TestBasicAuth(t *testing.T) {
	enc := func(s string) string { return "Basic " + base64.StdEncoding.EncodeToString([]byte(s)) }
	if got := basicAuth(Config{User: "u", Password: NewSecret("p")}); got != enc("u:p") {
		t.Fatal(got)
	}
	if got := basicAuth(Config{User: "sa@x", Token: NewSecret("tok")}); got != enc("sa@x:tok") {
		t.Fatal(got)
	}
	if got := basicAuth(Config{Token: NewSecret("Basic xyz")}); got != "Basic xyz" {
		t.Fatal(got)
	}
}

func TestClientCore(t *testing.T) {
	m := newMock(t, map[string][]string{
		"GET /healthz":            {"health.ok"},
		"GET /api/default/config": {"ping.ok"},
	})

	t.Run("health ping version", func(t *testing.T) {
		c := newOOClient(t, m.URL, nil)
		if hs, err := c.Health(t.Context()); err != nil || !hs.Healthy || hs.Status != "ok" {
			t.Fatalf("health %+v %v", hs, err)
		}
		if m.last().Header.Get("Authorization") != "" {
			t.Fatal("health must not send credentials")
		}
		if d, err := c.Ping(t.Context()); err != nil || d <= 0 {
			t.Fatalf("ping %s %v", d, err)
		}
		want := "Basic " + base64.StdEncoding.EncodeToString([]byte(testUser+":"+testPassword))
		if got := m.last().Header.Get("Authorization"); got != want {
			t.Fatalf("auth header %q", got)
		}
		if ua := m.last().Header.Get("User-Agent"); !strings.HasPrefix(ua, "grow-codes-openobserve/") || !strings.HasSuffix(ua, "(go)") {
			t.Fatalf("UA %q", ua)
		}
		if v, err := c.ServerVersion(t.Context()); err != nil || v != "v1.1.2" {
			t.Fatalf("version %q %v", v, err)
		}
		if c.Config().URL != m.URL || c.Config().Org != DefaultOrg {
			t.Fatal("Config()")
		}
	})

	t.Run("core.close.idempotent", func(t *testing.T) {
		c := newOOClient(t, m.URL, nil)
		lc, err := c.NewLogClient("s", LogClientOptions{})
		if err != nil {
			t.Fatal(err)
		}
		first, second := c.Close(), c.Close()
		if first != nil || second != nil {
			t.Fatal("close")
		}
		if _, err := c.Ping(t.Context()); !errors.Is(err, ErrCancelled) {
			t.Fatalf("after close: %v", err)
		}
		if lc.Close(t.Context()) != nil {
			t.Fatal("log client already closed by Client.Close")
		}
	})

	t.Run("close reports log client flush errors", func(t *testing.T) {
		bad := newMock(t, map[string][]string{"POST /api/default/s/_json": {"ingestJson.401"}})
		c := newOOClient(t, bad.URL, nil)
		lc, _ := c.NewLogClient("s", LogClientOptions{Batch: BatchOptions{OnError: func(error, []LogRecord) {}}})
		lc.Send(t.Context(), LogInput{Message: "x"})
		_ = c.Close()
	})

	t.Run("core.health.unreachable", func(t *testing.T) {
		c := newOOClient(t, "http://"+closedAddr(t), func(cfg *Config) { cfg.ConnectTimeout = time.Second })
		if hs, err := c.Health(t.Context()); err != nil || hs.Healthy || hs.Status != "unreachable" {
			t.Fatalf("hs=%+v err=%v", hs, err)
		}
	})

	t.Run("from env", func(t *testing.T) {
		t.Setenv("OPENOBSERVE_URL", m.URL)
		t.Setenv("OPENOBSERVE_USER", testUser)
		t.Setenv("OPENOBSERVE_PASSWORD", testPassword)
		c, err := NewClientFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		if _, err := c.Ping(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Setenv(EnvPrefix+"URL", "mailto:x")
		if _, err := NewClientFromEnv(); !errors.Is(err, ErrConfig) {
			t.Fatalf("bad env: %v", err)
		}
	})

	t.Run("bad tls config", func(t *testing.T) {
		_, err := NewClient(Config{User: "u", Password: NewSecret("p"), CommonConfig: CommonConfig{TLS: TLSConfig{CACertPEM: "not pem"}}})
		if !errors.Is(err, ErrConfig) {
			t.Fatal(err)
		}
	})
}

func TestErrorMapping(t *testing.T) {
	cases := []struct {
		fixture string
		code    ErrorCode
		text    string
		reqID   string
	}{
		{"ping.401", ErrorCodeAuthentication, "Unauthorized Access", ""},
		{"search.400_stream_not_found", ErrorCodeNotFound, "Search stream not found", "01a1f050f79b75028dc692d5e926b1aa"},
		{"search.400_unknown_field", ErrorCodeValidation, "unknown field 'nocol'", "01a1f050f79c7371a52fd2c60aa5a0d2"},
		{"search.404_unknown_org", ErrorCodeNotFound, "Organization not found", ""},
		{"search.422_missing_start_time", ErrorCodeValidation, "Failed to deserialize", ""},
		{"createServiceAccount.400_duplicate", ErrorCodeConflict, "User already exists", ""},
		{"getStreamSchema.404", ErrorCodeNotFound, "stream not found", ""},
		{"otlpLogs.400_invalid_protobuf", ErrorCodeValidation, "", ""},
		{"getDependencies.404_oss", ErrorCodeNotFound, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.fixture, func(t *testing.T) {
			m := newMock(t, map[string][]string{"GET /api/default/config": {tc.fixture}})
			c := newOOClient(t, m.URL, nil)
			_, err := c.Ping(t.Context())
			e := asError(t, err)
			if e.Code != tc.code || !strings.Contains(e.Message, tc.text) || e.RequestID != tc.reqID || !strings.HasPrefix(e.Message, "ping: ") {
				t.Fatalf("%+v", e)
			}
			if strings.Contains(e.Message, testPassword) {
				t.Fatal("secret in message")
			}
		})
	}

	t.Run("core.redaction.error-text", func(t *testing.T) {
		m := newMock(t, nil)
		m.handle("GET /api/default/config", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":400,"message":"bad password ` + testPassword + `"}`))
		})
		_, err := newOOClient(t, m.URL, nil).Ping(t.Context())
		if e := asError(t, err); strings.Contains(e.Message, testPassword) || !strings.Contains(e.Message, Redacted) {
			t.Fatalf("%v", e)
		}
	})

	t.Run("retryable statuses keep retry-after", func(t *testing.T) {
		m := newMock(t, nil)
		m.handle("GET /api/default/config", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "120")
			w.WriteHeader(http.StatusTooManyRequests)
		})
		_, err := newOOClient(t, m.URL, nil).Ping(t.Context())
		if e := asError(t, err); e.Code != ErrorCodeRateLimited || !e.Retryable || e.RetryAfter != 120*time.Second {
			t.Fatalf("%+v", e)
		}
	})

	t.Run("non-status errors pass through", func(t *testing.T) {
		c := newOOClient(t, m0(t), nil)
		if err := c.mapError("op", errors.New("plain"), nil); err.Error() != "plain" {
			t.Fatal(err)
		}
	})

	t.Run("json body without message", func(t *testing.T) {
		m := newMock(t, nil)
		m.handle("GET /api/default/config", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"nested text"}`))
		})
		_, err := newOOClient(t, m.URL, nil).Ping(t.Context())
		if e := asError(t, err); !strings.Contains(e.Message, "nested text") {
			t.Fatalf("%v", e)
		}
	})

	t.Run("multi-line text keeps first line", func(t *testing.T) {
		m := newMock(t, nil)
		m.handle("GET /api/default/config", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("first line\nsecond line"))
		})
		_, err := newOOClient(t, m.URL, nil).Ping(t.Context())
		if e := asError(t, err); !strings.Contains(e.Message, "first line") || strings.Contains(e.Message, "second") {
			t.Fatalf("%v", e)
		}
	})

	t.Run("invalid JSON success body", func(t *testing.T) {
		m := newMock(t, nil)
		m.handle("GET /api/default/config", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("nope")) })
		if _, err := newOOClient(t, m.URL, nil).ServerVersion(t.Context()); !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
	})

	t.Run("no version", func(t *testing.T) {
		m := newMock(t, nil)
		m.handle("GET /api/default/config", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("{}")) })
		if _, err := newOOClient(t, m.URL, nil).ServerVersion(t.Context()); !errors.Is(err, ErrUnsupported) {
			t.Fatal(err)
		}
	})
}

// m0 returns the URL of a mock without routes.
func m0(t *testing.T) string { return newMock(t, nil).URL }

func TestCaptureTransport(t *testing.T) {
	// Without a capture in the context the response passes through untouched.
	m := newMock(t, map[string][]string{"GET /x": {"ping.401"}})
	ct := &captureTransport{base: http.DefaultTransport}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, m.URL+"/x", nil)
	resp, err := ct.RoundTrip(req)
	if err != nil || resp.StatusCode != 401 {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	// Transport errors are returned as is.
	req, _ = http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+closedAddr(t)+"/x", nil)
	if _, err := ct.RoundTrip(req); err == nil {
		t.Fatal("expected error")
	}
}

// newTestClient is the client constructor the generated core tests use
// (core_tls_test.go): Basic credentials and fast retries.
func newTestClient(t *testing.T, url string, mutate func(*Config)) *Client {
	t.Helper()
	return newOOClient(t, url, func(cfg *Config) {
		cfg.Retry = fastRetry(3)
		if mutate != nil {
			mutate(cfg)
		}
	})
}
