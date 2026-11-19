// Service-owned: generated once by sdk-kit, then maintained in this repository.

import {
  type CommonConfig,
  type CommonConfigOptions,
  type CommonVendorEnv,
  ConfigError,
  type EnvMap,
  EnvReader,
  type Hooks,
  type ClientLogger,
  REDACTED,
  type RetryPolicy,
  Secret,
  type TlsOptions,
  describeCommonConfig,
  mergeOptions,
  readCommonEnv,
  resolveCommonConfig,
  secretValues,
  validateCommonConfig,
} from "./core/index.js";

const INSPECT = Symbol.for("nodejs.util.inspect.custom");

/** Port text from the stack catalog; the default URL uses its first number. */
const CATALOG_PORT = "5080";

/** Options accepted by {@link OpenObserveConfig} (all optional). */
export interface OpenObserveConfigOptions extends CommonConfigOptions {
  /** Organization (env `OPENOBSERVE_ORG`). Default `default`. */
  org?: string;
  /**
   * User e-mail for HTTP Basic auth: a root/user login or a service-account e-mail
   * (env `OPENOBSERVE_USER`).
   */
  user?: string;
  /** Password of {@link user} (env `OPENOBSERVE_PASSWORD`). Mutually exclusive with `token`. */
  password?: string | Secret;
  /**
   * Service-account token (env `OPENOBSERVE_TOKEN`), sent as `Basic base64(user:token)`.
   * A value that starts with `Basic ` is a pre-encoded header and is sent verbatim (no `user`).
   * OpenObserve does not accept `Bearer` tokens.
   */
  token?: string | Secret;
  /** Default log stream (env `OPENOBSERVE_STREAM`). Default `default`. */
  stream?: string;
  /** Default traces stream (env `OPENOBSERVE_TRACES_STREAM`). Default `default`. */
  tracesStream?: string;
}

/** Vendor variable names, read after `GROW_OPENOBSERVE_<NAME>`. */
export const VENDOR_ENV: CommonVendorEnv & {
  org: readonly string[];
  user: readonly string[];
  password: readonly string[];
  token: readonly string[];
  stream: readonly string[];
  tracesStream: readonly string[];
} = {
  url: ["OPENOBSERVE_URL"],
  org: ["OPENOBSERVE_ORG"],
  user: ["OPENOBSERVE_USER"],
  password: ["OPENOBSERVE_PASSWORD"],
  token: ["OPENOBSERVE_TOKEN"],
  stream: ["OPENOBSERVE_STREAM"],
  tracesStream: ["OPENOBSERVE_TRACES_STREAM"],
};

/** Server-side stream name normalization: lower case, every non `[a-z0-9_]` character → `_`. */
export function normalizeStreamName(name: string): string {
  return name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "_");
}

/** Immutable configuration of {@link OpenObserveClient}. */
export class OpenObserveConfig implements CommonConfig {
  /** Default endpoint. */
  static readonly DEFAULT_URL = `http://127.0.0.1:${/\d+/.exec(CATALOG_PORT)?.[0] ?? "8080"}`;
  /** Default organization. */
  static readonly DEFAULT_ORG = "default";
  /** Default log and traces stream (the server's OTLP default). */
  static readonly DEFAULT_STREAM = "default";
  /** Prefix of every environment variable. */
  static readonly ENV_PREFIX = "GROW_OPENOBSERVE_";

  readonly url: string;
  readonly connectTimeoutMs: number;
  readonly requestTimeoutMs: number;
  readonly totalTimeoutMs: number;
  readonly retry: RetryPolicy;
  readonly tls: Readonly<TlsOptions>;
  readonly proxy?: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly userAgentSuffix?: string;
  readonly hooks?: Hooks;
  readonly logger?: ClientLogger;
  /** Organization identifier. */
  readonly org: string;
  /** Basic-auth user (e-mail). */
  readonly user?: string;
  /** Password of {@link user}. */
  readonly password?: Secret;
  /** Service-account token or pre-encoded `Basic …` header. */
  readonly token?: Secret;
  /** Default log stream (normalized). */
  readonly stream: string;
  /** Default traces stream (normalized). */
  readonly tracesStream: string;

  /** Applies defaults, validates (throws {@link ConfigError}) and freezes. */
  constructor(options: OpenObserveConfigOptions = {}) {
    const common = resolveCommonConfig(options, OpenObserveConfig.DEFAULT_URL);
    this.url = common.url;
    this.connectTimeoutMs = common.connectTimeoutMs;
    this.requestTimeoutMs = common.requestTimeoutMs;
    this.totalTimeoutMs = common.totalTimeoutMs;
    this.retry = common.retry;
    this.tls = common.tls;
    this.headers = common.headers;
    if (common.proxy !== undefined) this.proxy = common.proxy;
    if (common.userAgentSuffix !== undefined) this.userAgentSuffix = common.userAgentSuffix;
    if (common.hooks !== undefined) this.hooks = common.hooks;
    if (common.logger !== undefined) this.logger = common.logger;
    this.org = (options.org ?? OpenObserveConfig.DEFAULT_ORG).trim();
    const user = options.user?.trim();
    if (user !== undefined && user !== "") this.user = user;
    const password = Secret.from(options.password);
    if (password !== undefined) this.password = password;
    const token = Secret.from(options.token);
    if (token !== undefined) this.token = token;
    this.stream = normalizeStreamName(options.stream ?? OpenObserveConfig.DEFAULT_STREAM);
    this.tracesStream = normalizeStreamName(
      options.tracesStream ?? OpenObserveConfig.DEFAULT_STREAM,
    );
    this.validate();
    Object.freeze(this);
  }

  /** Returns `options` when it already is a config, otherwise builds one. */
  static from(options: OpenObserveConfigOptions = {}): OpenObserveConfig {
    if (options instanceof OpenObserveConfig) return options;
    return new OpenObserveConfig(options);
  }

  /**
   * Builds a config from the environment. Precedence: explicit `overrides` >
   * `GROW_OPENOBSERVE_<NAME>` > `OPENOBSERVE_<NAME>` ({@link VENDOR_ENV}) > defaults.
   *
   * An explicit `password` override drops a `token` from the environment and vice versa, so
   * overriding the credential kind never trips the "password and token" rule.
   */
  static fromEnv(
    overrides: OpenObserveConfigOptions = {},
    env: EnvMap = process.env,
  ): OpenObserveConfig {
    const reader = new EnvReader(OpenObserveConfig.ENV_PREFIX, env);
    const fromEnv: OpenObserveConfigOptions = readCommonEnv(reader, VENDOR_ENV);
    const org = reader.string("ORG", VENDOR_ENV.org);
    if (org !== undefined) fromEnv.org = org;
    const user = reader.string("USER", VENDOR_ENV.user);
    if (user !== undefined) fromEnv.user = user;
    const password = reader.secret("PASSWORD", VENDOR_ENV.password);
    if (password !== undefined && overrides.token === undefined) fromEnv.password = password;
    const token = reader.secret("TOKEN", VENDOR_ENV.token);
    if (token !== undefined && overrides.password === undefined) fromEnv.token = token;
    const stream = reader.string("STREAM", VENDOR_ENV.stream);
    if (stream !== undefined) fromEnv.stream = stream;
    const tracesStream = reader.string("TRACES_STREAM", VENDOR_ENV.tracesStream);
    if (tracesStream !== undefined) fromEnv.tracesStream = tracesStream;
    const merged = mergeOptions<OpenObserveConfigOptions>(fromEnv, overrides);
    return new OpenObserveConfig(merged);
  }

  /** Throws {@link ConfigError} when a setting is invalid. */
  validate(): void {
    validateCommonConfig(this);
    if (this.org === "" || /[/?#\s]/.test(this.org)) {
      throw new ConfigError("invalid configuration: org must be a non-empty identifier");
    }
    if (this.stream === "")
      throw new ConfigError("invalid configuration: stream must not be blank");
    if (this.tracesStream === "") {
      throw new ConfigError("invalid configuration: tracesStream must not be blank");
    }
    if (this.password !== undefined && this.token !== undefined) {
      throw new ConfigError("invalid configuration: set either password or token, not both");
    }
    if (this.token?.reveal().trim() === "") {
      throw new ConfigError("invalid configuration: credentials must not be blank");
    }
    const preEncoded = this.token !== undefined && isPreEncoded(this.token.reveal());
    if ((this.password !== undefined || (this.token !== undefined && !preEncoded)) && !this.user) {
      throw new ConfigError("invalid configuration: user is required with password or token");
    }
    if (this.user !== undefined && this.password === undefined && this.token === undefined) {
      throw new ConfigError("invalid configuration: user requires password or token");
    }
  }

  /** Whether credentials are configured. */
  get hasCredentials(): boolean {
    return this.password !== undefined || this.token !== undefined;
  }

  /**
   * The `Authorization` header value (`Basic …`), or `undefined` without credentials. The result
   * is a credential: never log it.
   */
  authorization(): string | undefined {
    if (this.token !== undefined) {
      const token = this.token.reveal().trim();
      if (isPreEncoded(token)) return `Basic ${token.slice(6).trim()}`;
      return basic(this.user ?? "", token);
    }
    if (this.password !== undefined) return basic(this.user ?? "", this.password.reveal());
    return undefined;
  }

  /** Credential values to redact from error messages and logs. */
  knownSecrets(): string[] {
    const out = secretValues(this.password, this.token);
    const header = this.authorization();
    if (header !== undefined) out.push(header.slice(6));
    return out;
  }

  /** Debug form with every credential redacted. */
  toJSON(): Record<string, unknown> {
    return {
      ...describeCommonConfig(this),
      org: this.org,
      user: this.user ?? null,
      password: this.password === undefined ? null : REDACTED,
      token: this.token === undefined ? null : REDACTED,
      stream: this.stream,
      tracesStream: this.tracesStream,
    };
  }

  toString(): string {
    return `OpenObserveConfig(${JSON.stringify(this.toJSON())})`;
  }

  [INSPECT](): string {
    return this.toString();
  }
}

function isPreEncoded(token: string): boolean {
  return /^basic\s/i.test(token.trim());
}

function basic(user: string, secret: string): string {
  return `Basic ${Buffer.from(`${user}:${secret}`, "utf8").toString("base64")}`;
}
