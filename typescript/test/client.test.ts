import { inspect } from "node:util";
import { afterEach, describe, expect, it } from "vitest";
import {
  AuthenticationError,
  ConfigError,
  NotFoundError,
  OpenObserveClient,
  OpenObserveConfig,
  OpenObserveError,
  REDACTED,
  RetryPolicy,
  Secret,
  UnsupportedError,
  VENDOR_ENV,
  normalizeStreamName,
} from "../src/index.js";
import { fx } from "./helpers/fixtures.js";
import { type MockServer, closedPort, startServer } from "./helpers/server.js";

const servers: MockServer[] = [];
afterEach(async () => {
  await Promise.all(servers.splice(0).map((s) => s.close()));
});

async function server(...args: Parameters<typeof startServer>): Promise<MockServer> {
  const s = await startServer(...args);
  servers.push(s);
  return s;
}

const fast = { retry: { maxRetries: 1, initialBackoffMs: 1 } };
const creds = { user: "admin@stack.local", password: "Gw1-secret-pass" };
const basic = (u: string, p: string): string =>
  `Basic ${Buffer.from(`${u}:${p}`).toString("base64")}`;

describe("OpenObserveConfig", () => {
  it("uses defaults and freezes", () => {
    const c = new OpenObserveConfig();
    expect(c.url).toBe("http://127.0.0.1:5080");
    expect(c.org).toBe("default");
    expect(c.stream).toBe("default");
    expect(c.tracesStream).toBe("default");
    expect(c.retry).toBeInstanceOf(RetryPolicy);
    expect(c.hasCredentials).toBe(false);
    expect(c.authorization()).toBeUndefined();
    expect(c.knownSecrets()).toEqual([]);
    expect(Object.isFrozen(c)).toBe(true);
    expect(OpenObserveConfig.from(c)).toBe(c);
    expect(OpenObserveConfig.ENV_PREFIX).toBe("GROW_OPENOBSERVE_");
    expect(VENDOR_ENV.url).toEqual(["OPENOBSERVE_URL"]);
  });

  it("builds Basic auth from user + password and user + token", () => {
    const p = new OpenObserveConfig(creds);
    expect(p.authorization()).toBe(basic(creds.user, creds.password));
    expect(p.knownSecrets()).toContain(creds.password);
    const t = new OpenObserveConfig({ user: "sa@x.io", token: new Secret("tok-123456") });
    expect(t.authorization()).toBe(basic("sa@x.io", "tok-123456"));
    expect(t.hasCredentials).toBe(true);
  });

  it("sends a pre-encoded Basic token verbatim without a user", () => {
    const c = new OpenObserveConfig({ token: "basic   YWJjOmRlZg==" });
    expect(c.authorization()).toBe("Basic YWJjOmRlZg==");
    expect(c.knownSecrets()).toContain("YWJjOmRlZg==");
  });

  it("normalizes stream names like the server", () => {
    expect(normalizeStreamName(" Sdk-Mixed.x ")).toBe("sdk_mixed_x");
    const c = new OpenObserveConfig({ stream: "App.Logs", tracesStream: "My-Traces" });
    expect(c.stream).toBe("app_logs");
    expect(c.tracesStream).toBe("my_traces");
  });

  it.each([
    [{ org: "" }, /org/],
    [{ org: "a/b" }, /org/],
    [{ stream: "  " }, /stream/],
    [{ tracesStream: " " }, /tracesStream/],
    [{ user: "u", password: "p", token: "t" }, /either password or token/],
    [{ user: "u", password: "" }, /requires password or token/],
    [{ user: "u", token: "  " }, /blank/],
    [{ password: "p" }, /user is required/],
    [{ token: "raw" }, /user is required/],
    [{ user: "u" }, /requires password or token/],
    [{ url: "ftp://x" }, /url/i],
  ])("rejects %o", (options, message) => {
    expect(() => new OpenObserveConfig(options)).toThrow(ConfigError);
    expect(() => new OpenObserveConfig(options)).toThrow(message);
  });

  it("redacts credentials in every debug form", () => {
    const c = new OpenObserveConfig({ ...creds, headers: { "x-extra": "v" } });
    for (const text of [c.toString(), JSON.stringify(c), inspect(c)]) {
      expect(text).not.toContain(creds.password);
      expect(text).toContain(REDACTED);
      expect(text).toContain(creds.user);
    }
    expect(new OpenObserveConfig({ token: "Basic abcd" }).toJSON().password).toBeNull();
    expect(new OpenObserveConfig().toJSON().token).toBeNull();
  });

  it("fromEnv: GROW_ beats vendor beats default; overrides win", () => {
    const env = {
      GROW_OPENOBSERVE_URL: "http://grow:5080",
      OPENOBSERVE_URL: "http://vendor:5080",
      OPENOBSERVE_ORG: "acme",
      OPENOBSERVE_USER: "root@x.io",
      OPENOBSERVE_PASSWORD: "pw-12345",
      OPENOBSERVE_STREAM: "app",
      GROW_OPENOBSERVE_TRACES_STREAM: "tr",
      GROW_OPENOBSERVE_REQUEST_TIMEOUT: "5s",
    };
    const c = OpenObserveConfig.fromEnv({}, env);
    expect(c.url).toBe("http://grow:5080");
    expect(c.org).toBe("acme");
    expect(c.user).toBe("root@x.io");
    expect(c.password?.reveal()).toBe("pw-12345");
    expect(c.stream).toBe("app");
    expect(c.tracesStream).toBe("tr");
    expect(c.requestTimeoutMs).toBe(5000);
    expect(OpenObserveConfig.fromEnv({ org: "other" }, env).org).toBe("other");
    expect(OpenObserveConfig.fromEnv({}, {}).url).toBe(OpenObserveConfig.DEFAULT_URL);
  });

  it("fromEnv: an explicit token drops the env password and vice versa", () => {
    const env = {
      OPENOBSERVE_USER: "u@x.io",
      OPENOBSERVE_PASSWORD: "pw-1",
      OPENOBSERVE_TOKEN: "t-1",
    };
    expect(() => OpenObserveConfig.fromEnv({}, env)).toThrow(/either password or token/);
    const t = OpenObserveConfig.fromEnv({ token: "t-2" }, env);
    expect(t.password).toBeUndefined();
    expect(t.token?.reveal()).toBe("t-2");
    const p = OpenObserveConfig.fromEnv({ password: "pw-2" }, env);
    expect(p.token).toBeUndefined();
    expect(p.password?.reveal()).toBe("pw-2");
  });

  it("fromEnv: invalid values name the variable, never the value", () => {
    expect(() =>
      OpenObserveConfig.fromEnv({}, { GROW_OPENOBSERVE_MAX_RETRIES: "lots-secret" }),
    ).toThrow(/GROW_OPENOBSERVE_MAX_RETRIES/);
  });
});

describe("OpenObserveClient core", () => {
  it("health maps /healthz", async () => {
    const s = await server([fx("health.ok")]);
    const client = new OpenObserveClient({ url: s.url });
    expect(await client.health()).toEqual({ healthy: true, status: "ok", details: {} });
    expect(s.requests[0]?.url).toBe("/healthz");
    expect(s.requests[0]?.headers.authorization).toBeUndefined();
    await client.close();
  });

  it("health tolerates bodies without status and reports unreachable", async () => {
    const s = await server([{ status: 200, body: "" }]);
    const client = new OpenObserveClient({ url: s.url });
    expect((await client.health()).status).toBe("ok");
    const down = new OpenObserveClient({ url: `http://127.0.0.1:${await closedPort()}`, ...fast });
    const h = await down.health();
    expect(h.healthy).toBe(false);
    expect(h.status).toBe("unreachable");
    await Promise.all([client.close(), down.close()]);
  });

  it("ping and serverVersion use the authenticated /api/{org}/config", async () => {
    const s = await server([fx("ping.ok")]);
    const client = new OpenObserveClient({ url: s.url, org: "acme", ...creds });
    expect(await client.ping()).toBeGreaterThanOrEqual(0);
    expect(await client.serverVersion()).toBe("v1.1.2");
    expect(s.requests[0]?.url).toBe("/api/acme/config");
    expect(s.requests[0]?.headers.authorization).toBe(basic(creds.user, creds.password));
    expect(s.requests[0]?.headers["user-agent"]).toMatch(
      /^grow-codes-openobserve\/\S+ \(typescript\)$/,
    );
    await client.close();
  });

  it("ping maps the plain-text 401 and never leaks credentials", async () => {
    const s = await server([fx("ping.401")]);
    const client = new OpenObserveClient({ url: s.url, ...creds });
    const err = await client.ping().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(AuthenticationError);
    expect((err as OpenObserveError).httpStatus).toBe(401);
    expect((err as OpenObserveError).message).toContain("Unauthorized Access");
    expect(JSON.stringify(err)).not.toContain(creds.password);
    await client.close();
  });

  it("serverVersion throws UnsupportedError without a version", async () => {
    const s = await server([{ status: 200, body: { build_type: "opensource" } }]);
    const client = new OpenObserveClient({ url: s.url, ...creds });
    await expect(client.serverVersion()).rejects.toBeInstanceOf(UnsupportedError);
    await client.close();
  });

  it("rejects invalid JSON bodies with ValidationError", async () => {
    const s = await server([{ status: 200, body: "{not json" }]);
    const client = new OpenObserveClient({ url: s.url, ...creds });
    await expect(client.serverVersion()).rejects.toThrow(/invalid JSON/);
    await client.close();
  });

  it("maps 404 plain text to NotFoundError", async () => {
    const s = await server([fx("search.404_unknown_org")]);
    const client = new OpenObserveClient({ url: s.url, ...creds });
    await expect(client.logs.search('SELECT * FROM "x"')).rejects.toBeInstanceOf(NotFoundError);
    await client.close();
  });

  it("fromEnv builds a client; close is idempotent and closes log clients", async () => {
    const s = await server([fx("ingestJson.ok")]);
    const prev = process.env.GROW_OPENOBSERVE_URL;
    process.env.GROW_OPENOBSERVE_URL = s.url;
    try {
      const client = OpenObserveClient.fromEnv({ ...creds, stream: "sdk_logs_6moiazbq" });
      expect(client.config.url).toBe(s.url);
      expect(client.transport).toBeDefined();
      const logs = client.logClient({ batch: { flushIntervalMs: 60_000 } });
      logs.send({ message: "bye" });
      await client.close();
      await client.close();
      expect(logs.stats().sent).toBe(1);
      expect(s.requests[0]?.url).toBe("/api/default/sdk_logs_6moiazbq/_json");
      await expect(client.ping()).rejects.toThrow(/closed/);
    } finally {
      if (prev === undefined) delete process.env.GROW_OPENOBSERVE_URL;
      else process.env.GROW_OPENOBSERVE_URL = prev;
    }
  });

  it("logQuery and traceQuery default to the config streams", () => {
    const client = new OpenObserveClient({ stream: "A", tracesStream: "B" });
    expect(client.logQuery().stream).toBe("a");
    expect(client.logQuery("Other").stream).toBe("other");
    expect(client.traces.stream).toBe("b");
    expect(client.traceQuery("T.2").stream).toBe("t_2");
  });
});
