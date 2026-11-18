package integration

import (
	"context"
	"strings"
	"testing"

	openobserve "github.com/grow-codes-packages/openobserve/go"
	"github.com/grow-codes-packages/openobserve/go/testkit"
)

// capabilities the deployment under test offers (see conformance "requires").
// "dependencies" is left out: the service graph is an enterprise feature of
// OpenObserve (docs/compatibility.md).
var capabilities = []string{"version", "query", "tail", "labels", "streams", "services"}

// categories runs "core" plus this service's categories.
func categories() []string {
	return append([]string{"core"}, strings.Split("logging,tracing", ",")...)
}

// factories builds the conformance targets from the bootstrap environment
// (OPENOBSERVE_USER/PASSWORD, OPENOBSERVE_STREAM, OPENOBSERVE_TRACES_STREAM).
func factories(t *testing.T) testkit.Factories {
	t.Helper()
	return testkit.Factories{
		"core": testkit.CoreFactory(func(context.Context) (testkit.CoreTarget, error) {
			return openobserve.NewClientFromEnv()
		}),
		"logging": testkit.LoggingFactory(func(_ context.Context, opts openobserve.BatchOptions) (testkit.LoggingTarget, error) {
			c, err := openobserve.NewClientFromEnv()
			if err != nil {
				return testkit.LoggingTarget{}, err
			}
			lc, err := c.NewLogClient("", openobserve.LogClientOptions{
				Batch:    opts,
				Resource: map[string]openobserve.AttrValue{"service.name": "grow-conformance-go"},
			})
			if err != nil {
				return testkit.LoggingTarget{}, err
			}
			return testkit.LoggingTarget{Client: lc, Query: c.LogQuery("")}, nil
		}),
		"tracing": testkit.TracingFactory(func(ctx context.Context, serviceName string) (testkit.TracingTarget, error) {
			c, err := openobserve.NewClientFromEnv()
			if err != nil {
				return testkit.TracingTarget{}, err
			}
			h, err := c.InitTracing(ctx, openobserve.TracingConfig{ServiceName: serviceName, DisableSetGlobal: true})
			if err != nil {
				return testkit.TracingTarget{}, err
			}
			return testkit.TracingTarget{Handle: h, Query: c.Traces()}, nil
		}),
	}
}
