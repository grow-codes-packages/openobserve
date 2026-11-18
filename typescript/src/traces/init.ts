// Service-owned. initTracing for OpenObserve: the official OTLP exporters pointed at
// `/api/{org}/v1/traces` with Basic auth and the `stream-name` header (API.md §5).

import { readFileSync } from "node:fs";
import { Metadata } from "@grpc/grpc-js";
import { OTLPTraceExporter as OtlpGrpcExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { OTLPTraceExporter as OtlpJsonExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { OTLPTraceExporter as OtlpProtoExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { CompressionAlgorithm } from "@opentelemetry/otlp-exporter-base";
import type { SpanExporter } from "@opentelemetry/sdk-trace-base";
import { LANGUAGE, PACKAGE_KEY, SDK_VERSION, type TlsOptions } from "../core/index.js";
import {
  initTracingWith,
  type InitTracingOptions,
  type ResolvedTracingConfig,
  type TracingConfig,
  type TracingHandle,
} from "../tracing/index.js";
import { OpenObserveConfig, type OpenObserveConfigOptions } from "../config.js";
import { streamName } from "../http.js";

/** {@link TracingConfig} plus where to send the spans. */
export interface OpenObserveTracingConfig extends TracingConfig {
  /**
   * OpenObserve connection (URL, org, credentials, traces stream). Default:
   * `OpenObserveConfig.fromEnv()`.
   */
  openobserve?: OpenObserveConfig | OpenObserveConfigOptions;
  /** Traces stream (header `stream-name`). Default: the config's `tracesStream`. */
  stream?: string;
}

/** Default OTLP/gRPC port of OpenObserve. */
export const OPENOBSERVE_GRPC_PORT = "5081";

/** The OTLP/HTTP traces endpoint: `<url>/api/{org}/v1/traces`. */
export function tracesEndpoint(config: OpenObserveConfig): string {
  const base = config.url.endsWith("/") ? config.url : `${config.url}/`;
  return new URL(`api/${encodeURIComponent(config.org)}/v1/traces`, base).toString();
}

/** The default OTLP/gRPC endpoint: the URL's host with port 5081 when it uses 5080. */
export function grpcEndpoint(config: OpenObserveConfig): string {
  const url = new URL(config.url);
  if (url.port === "5080" || url.port === "") url.port = OPENOBSERVE_GRPC_PORT;
  return `${url.protocol}//${url.host}`;
}

/**
 * Service defaults merged under the caller's {@link TracingConfig}: OTLP/HTTP protobuf to
 * {@link tracesEndpoint} with `authorization` (Basic) and `stream-name` headers.
 */
export function openObserveTracingDefaults(
  config: OpenObserveConfig,
  stream: string = config.tracesStream,
): Partial<TracingConfig> {
  const headers: Record<string, string> = {
    "stream-name": streamName(stream, config.tracesStream, "tracing"),
  };
  const auth = config.authorization();
  if (auth !== undefined) headers.authorization = auth;
  return { protocol: "http/protobuf", endpoint: tracesEndpoint(config), headers };
}

function readPem(path: string | undefined): string | undefined {
  return path === undefined ? undefined : readFileSync(path, "utf8");
}

function agentOptions(tls: Readonly<TlsOptions>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  const ca = [tls.caCertPem, readPem(tls.caCertPath)].filter((v) => v !== undefined);
  if (ca.length > 0) out.ca = ca;
  const cert = readPem(tls.clientCertPath);
  if (cert !== undefined) out.cert = cert;
  const key = readPem(tls.clientKeyPath);
  if (key !== undefined) out.key = key;
  if (tls.insecureSkipVerify === true) out.rejectUnauthorized = false;
  return out;
}

/**
 * Builds the span exporter for a resolved config: `http/protobuf` → OTLP/HTTP protobuf,
 * `native` → OTLP/HTTP JSON (OpenObserve's own ingestion format), `grpc` → OTLP/gRPC with the
 * `authorization`, `organization` and `stream-name` metadata.
 */
export function createOpenObserveSpanExporter(
  resolved: ResolvedTracingConfig,
  config: OpenObserveConfig,
): SpanExporter {
  const compression =
    resolved.compression === "gzip" ? CompressionAlgorithm.GZIP : CompressionAlgorithm.NONE;
  const tls = Object.keys(resolved.tls).length > 0 ? resolved.tls : config.tls;
  const userAgent = `grow-codes-${PACKAGE_KEY}/${SDK_VERSION} (${LANGUAGE})`;
  if (resolved.protocol === "grpc") {
    const metadata = new Metadata();
    for (const [k, v] of Object.entries(resolved.headers)) metadata.set(k.toLowerCase(), v);
    if (metadata.get("organization").length === 0) metadata.set("organization", config.org);
    return new OtlpGrpcExporter({
      url: resolved.endpoint ?? grpcEndpoint(config),
      metadata,
      compression,
      userAgent,
      timeoutMillis: resolved.batch.exportTimeoutMs,
    });
  }
  const options = {
    url: resolved.endpoint ?? tracesEndpoint(config),
    headers: { ...resolved.headers },
    compression,
    userAgent,
    timeoutMillis: resolved.batch.exportTimeoutMs,
    httpAgentOptions: agentOptions(tls),
  };
  return resolved.protocol === "native"
    ? new OtlpJsonExporter(options)
    : new OtlpProtoExporter(options);
}

/**
 * Sets up OpenTelemetry tracing that exports to OpenObserve (SPEC §5). Endpoint, auth and stream
 * come from `config.openobserve` (default `OpenObserveConfig.fromEnv()`); every
 * {@link TracingConfig} field can override them. Throws `ConfigError` for invalid settings.
 */
export function initTracing(
  config: OpenObserveTracingConfig,
  options: InitTracingOptions = {},
): TracingHandle {
  const { openobserve, stream, ...tracing } = config;
  const ooConfig =
    openobserve === undefined ? OpenObserveConfig.fromEnv() : OpenObserveConfig.from(openobserve);
  const defaults = { ...openObserveTracingDefaults(ooConfig, stream), ...options.defaults };
  if (tracing.endpoint === undefined && tracing.protocol === "grpc") {
    defaults.endpoint = grpcEndpoint(ooConfig);
  }
  return initTracingWith(tracing, (resolved) => createOpenObserveSpanExporter(resolved, ooConfig), {
    ...options,
    defaults,
  });
}
