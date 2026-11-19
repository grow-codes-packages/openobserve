// Core client against the real service: health, ping, serverVersion, close, credentials.

import assert from "node:assert/strict";
import { test } from "node:test";
import { OpenObserveClient, OpenObserveConfig, OpenObserveError } from "@grow-codes/openobserve";
import { rootClient, serviceAccountClient } from "./helpers.ts";

test("health reports a healthy service without credentials", async () => {
  const client = new OpenObserveClient({ url: process.env.GROW_OPENOBSERVE_URL! });
  try {
    const health = await client.health();
    assert.equal(health.healthy, true, JSON.stringify(health));
    assert.equal(health.status, "ok");
  } finally {
    await client.close();
  }
});

test("ping succeeds with root (user + password) and service-account (user + token) credentials", async () => {
  for (const client of [rootClient(), serviceAccountClient()]) {
    try {
      const latency = await client.ping();
      assert.equal(typeof latency, "number");
      assert.ok(latency >= 0);
    } finally {
      await client.close();
    }
  }
});

test("a pre-encoded Basic token works as OPENOBSERVE_TOKEN", async () => {
  const user = process.env.OPENOBSERVE_SA_EMAIL!;
  const token = process.env.OPENOBSERVE_SA_TOKEN!;
  const encoded = `Basic ${Buffer.from(`${user}:${token}`).toString("base64")}`;
  const client = new OpenObserveClient(
    new OpenObserveConfig({ url: process.env.GROW_OPENOBSERVE_URL!, token: encoded }),
  );
  try {
    assert.ok((await client.ping()) >= 0);
  } finally {
    await client.close();
  }
});

test("ping with a wrong password raises AUTHENTICATION and never leaks it", async () => {
  const wrong = "wrong-password-for-integration";
  const client = OpenObserveClient.fromEnv({ password: wrong });
  try {
    await assert.rejects(client.ping(), (err: unknown) => {
      assert.ok(err instanceof OpenObserveError);
      assert.equal(err.code, "AUTHENTICATION");
      assert.equal(err.httpStatus, 401);
      assert.ok(!err.message.includes(wrong));
      return true;
    });
  } finally {
    await client.close();
  }
});

test("serverVersion matches the bootstrap version; close is idempotent", async () => {
  const client = rootClient();
  try {
    const version = await client.serverVersion();
    assert.match(version, /^v?\d+\.\d+/);
    const expected = process.env.OPENOBSERVE_SERVER_VERSION;
    if (expected) assert.equal(version, expected);
  } finally {
    await client.close();
    await client.close();
  }
  await assert.rejects(client.ping(), /closed/);
});
