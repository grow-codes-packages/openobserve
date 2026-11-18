// Recorded OpenObserve v1.1.2 responses (test/fixtures, from the prep verify run).

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { parseJson } from "../../src/http.js";
import type { MockResponse } from "./server.js";

/** One recorded fixture. */
export interface Fixture {
  fixture: string;
  request: { method: string; path: string; headers: Record<string, string>; body: unknown };
  response: { status: number; headers: Record<string, string>; body: unknown };
}

const dir = fileURLToPath(new URL("../fixtures/", import.meta.url));

/** Loads `test/fixtures/<name>.json`. */
export function fixture(name: string): Fixture {
  return parseJson(readFileSync(`${dir}${name}.json`, "utf8")) as Fixture;
}

/** JSON text where exact 64-bit integers (kept as strings by `parseJson`) are numbers again. */
export function toJsonText(value: unknown): string {
  return JSON.stringify(value).replace(/"(-?\d{17,})"/g, "$1");
}

/** The recorded response of a fixture as a mock server response. */
export function fx(name: string, patch: (body: any) => unknown = (b) => b): MockResponse {
  const f = fixture(name);
  const body = patch(structuredClone(f.response.body));
  return {
    status: f.response.status,
    headers: { ...f.response.headers },
    body: typeof body === "string" ? body : toJsonText(body),
  };
}

