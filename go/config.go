package openobserve

import (
	"fmt"
	"strings"
)

// DefaultURL is the default endpoint of OpenObserve (REST, UI and OTLP/HTTP share it).
const DefaultURL = "http://127.0.0.1:5080"

// DefaultOrg is the default OpenObserve organization.
const DefaultOrg = "default"

// DefaultStream is the default log stream and traces stream (the server's own
// default for OTLP data without a stream-name header).
const DefaultStream = "default"

// vendorEnv lists the vendor variables consulted after GROW_OPENOBSERVE_*.
var vendorEnv = VendorEnv{URL: []string{"OPENOBSERVE_URL"}}

// Config configures a Client. Build it with a struct literal or ConfigFromEnv;
// zero values mean "use the default". Call Validate to check it.
//
// OpenObserve accepts HTTP Basic authentication only. Give User plus exactly one
// of Password (a user password) or Token (a service-account token, sent as
// Basic base64(User:Token)). A Token that starts with "Basic " is a pre-encoded
// Authorization value and is sent verbatim; User is then optional.
type Config struct {
	CommonConfig

	// Org is the organization identifier (default "default"; env ORG,
	// OPENOBSERVE_ORG).
	Org string
	// User is the user or service-account e-mail (env USER, OPENOBSERVE_USER).
	User string
	// Password is the user's password (env PASSWORD, OPENOBSERVE_PASSWORD).
	Password Secret
	// Token is a service-account token or a pre-encoded "Basic ..." header value
	// (env TOKEN, OPENOBSERVE_TOKEN).
	Token Secret
	// Stream is the default log stream (default "default"; env STREAM,
	// OPENOBSERVE_STREAM).
	Stream string
	// TracesStream is the stream spans are exported to and queried from (default
	// "default"; env TRACES_STREAM, OPENOBSERVE_TRACES_STREAM).
	TracesStream string
}

// ConfigFromEnv reads the configuration from the process environment.
func ConfigFromEnv() (Config, error) { return ConfigFromEnvWith(Config{}, nil) }

// ConfigFromEnvWith resolves explicit values > GROW_OPENOBSERVE_* > OPENOBSERVE_*
// variables > defaults. env replaces the process environment when non-nil. The
// result is validated.
func ConfigFromEnvWith(explicit Config, env map[string]string) (Config, error) {
	r := NewEnvReader(EnvPrefix, env)
	c := explicit
	if err := c.applyEnv(r, vendorEnv); err != nil {
		return Config{}, err
	}
	setString := func(dst *string, name string) {
		if *dst == "" {
			if v, ok := r.String(name, "OPENOBSERVE_"+name); ok {
				*dst = v
			}
		}
	}
	setSecret := func(dst *Secret, name string) {
		if dst.IsZero() {
			if v, ok := r.String(name, "OPENOBSERVE_"+name); ok {
				*dst = NewSecret(v)
			}
		}
	}
	setString(&c.Org, "ORG")
	setString(&c.User, "USER")
	setSecret(&c.Password, "PASSWORD")
	setSecret(&c.Token, "TOKEN")
	setString(&c.Stream, "STREAM")
	setString(&c.TracesStream, "TRACES_STREAM")
	c = c.withDefaults()
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// withDefaults returns a copy with every default applied.
func (c Config) withDefaults() Config {
	c.CommonConfig = c.CommonConfig.withDefaults(DefaultURL)
	if c.Org == "" {
		c.Org = DefaultOrg
	}
	if c.Stream == "" {
		c.Stream = DefaultStream
	}
	if c.TracesStream == "" {
		c.TracesStream = DefaultStream
	}
	return c
}

// preEncoded reports whether the token is a ready "Basic ..." header value.
func (c Config) preEncoded() bool {
	return strings.HasPrefix(c.Token.Reveal(), "Basic ")
}

// Validate checks the configuration and returns a CONFIG error: the common rules
// (SPEC §2.2), exactly one of Password or Token, User unless the token is
// pre-encoded, and an organization without path characters.
func (c Config) Validate() error {
	c = c.withDefaults()
	if err := c.validate(); err != nil {
		return err
	}
	if strings.ContainsAny(c.Org, "/?#% \t\r\n") {
		return ConfigError("org must not contain path characters or spaces")
	}
	switch {
	case !c.Password.IsZero() && !c.Token.IsZero():
		return ConfigError("set either password or token, not both")
	case c.Password.IsZero() && c.Token.IsZero():
		return ConfigError("password or token is required (OpenObserve uses HTTP Basic authentication)")
	case c.User == "" && !c.preEncoded():
		return ConfigError("user is required with a password or a raw service-account token")
	}
	if strings.TrimSpace(c.Stream) == "" || strings.TrimSpace(c.TracesStream) == "" {
		return ConfigError("stream names must not be blank")
	}
	return nil
}

// String renders the configuration with every credential redacted.
func (c Config) String() string {
	return fmt.Sprintf("Config{%s Org:%s User:%s Password:%s Token:%s Stream:%s TracesStream:%s}",
		c.describe(), c.Org, c.User, c.Password, c.Token, c.Stream, c.TracesStream)
}

// GoString renders the configuration with every credential redacted.
func (c Config) GoString() string { return c.String() }
