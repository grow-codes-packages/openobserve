// Shared helpers of the integration suite (real OpenObserve from docker/compose.yml).

import assert from "node:assert/strict";
import { OpenObserveClient } from "@grow-codes/openobserve";
import { loadBootstrapEnv, runId } from "./env.ts";

loadBootstrapEnv();

/** Unique lower-case suffix for names created by this run. */
export const suffix = `${runId.toLowerCase().replace(/[^a-z0-9]/g, "")}${Math.random().toString(36).slice(2, 8)}`;

/** A unique stream name. */
export function uniqueStream(prefix: string): string {
  return `ts_${prefix}_${suffix}`;
}

/** Client with the root credentials from /bootstrap/env. */
export function rootClient(): OpenObserveClient {
  return OpenObserveClient.fromEnv();
}

/** Client authenticated with the bootstrap service account (user + token). */
export function serviceAccountClient(): OpenObserveClient {
  const user = process.env.OPENOBSERVE_SA_EMAIL;
  const token = process.env.OPENOBSERVE_SA_TOKEN;
  assert.ok(user && token, "OPENOBSERVE_SA_EMAIL/OPENOBSERVE_SA_TOKEN must be set");
  return OpenObserveClient.fromEnv({ user, token });
}

/** Polls `fn` until `ok(value)` holds or the timeout passes; returns the last value. */
export async function eventually<T>(
  fn: () => Promise<T>,
  ok: (value: T) => boolean,
  timeoutMs = 20_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await fn();
    if (ok(value) || Date.now() > deadline) return value;
    await new Promise((r) => setTimeout(r, 250));
  }
}
