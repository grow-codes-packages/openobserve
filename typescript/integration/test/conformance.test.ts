// Runs the shared conformance scenarios (conformance/scenarios) against the real service with
// the core, logging and tracing factories. Declared capabilities:
//   core: version · logging: query · tracing: query, services
// Not declared: `dependencies` (enterprise-only in OpenObserve, see docs/compatibility.md).

import assert from "node:assert/strict";
import { before, test } from "node:test";
import { OpenObserveClient, initTracing, type BatchOptions } from "@grow-codes/openobserve";
import { formatReport, runConformance } from "@grow-codes/openobserve/testing";
import { loadBootstrapEnv, scenariosDir } from "./env.ts";

before(() => {
  loadBootstrapEnv();
});

test("conformance: core, logging, tracing", async () => {
  const report = await runConformance({
    scenariosDir,
    categories: ["core", "logging", "tracing"],
    factories: {
      core: () => OpenObserveClient.fromEnv(),
      logging: (ctx) => {
        const client = OpenObserveClient.fromEnv();
        const stream = `ts_conf_${ctx.run}`;
        return {
          client: client.logClient({ stream, batch: ctx.batchOptions as BatchOptions | undefined }),
          query: client.logQuery(stream),
        };
      },
      tracing: (ctx) => {
        const client = OpenObserveClient.fromEnv();
        const stream = `ts_conf_traces_${ctx.run}`;
        return {
          handle: initTracing({
            serviceName: `ts-conformance-${ctx.run}`,
            setGlobal: false,
            openobserve: client.config,
            stream,
          }),
          query: client.traceQuery(stream),
        };
      },
    },
    capabilities: { core: ["version"], logging: ["query"], tracing: ["query", "services"] },
  });
  console.log(formatReport(report));
  assert.equal(report.failed, 0, formatReport(report));
  assert.equal(report.skipped, 0, formatReport(report));
  assert.ok(report.passed >= 6);
});
