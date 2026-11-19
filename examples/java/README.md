# Java examples

Install the package into your local Maven repository, point it at a running OpenObserve and run an
example:

```bash
make -C ../../java pack            # builds and installs codes.grow.packages:openobserve
export GROW_OPENOBSERVE_URL=http://127.0.0.1:5080
export GROW_OPENOBSERVE_USER=admin@example.com
export GROW_OPENOBSERVE_PASSWORD=...   # or GROW_OPENOBSERVE_TOKEN=<service-account token>
mvn -q compile exec:java                                                  # BasicExample
mvn -q compile exec:java -Dexec.mainClass=codes.grow.packages.openobserve.examples.AsyncExample
mvn -q compile exec:java -Dexec.mainClass=codes.grow.packages.openobserve.examples.LogsAndTracesExample
```

OpenObserve uses HTTP Basic authentication only: a user with a password, or a service-account
e-mail (`USER`) with its token (`TOKEN`). The vendor variables `OPENOBSERVE_URL`,
`OPENOBSERVE_USER`, `OPENOBSERVE_PASSWORD`, `OPENOBSERVE_TOKEN`, `OPENOBSERVE_ORG` and
`OPENOBSERVE_STREAM` work too.

Against the published package, set `-Dsdk.version=<version>` (or edit `pom.xml`) instead of
installing it locally.

| Class | Shows |
| - | - |
| `BasicExample` | health, ping, server version, `logs().ingestJson`, SQL `logs().search`, error handling |
| `AsyncExample` | `…Async` methods, chaining ingest and search, cancellation |
| `HooksAndLoggingExample` | request hooks, an SDK logger, retries and timeouts |
| `LogsAndTracesExample` | batching `logClient()` + `Logger`, `logQuery().waitForLog`, stream schema, `initTracing` + `traceQuery()` |
