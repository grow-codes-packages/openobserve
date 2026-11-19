// Service-owned. Admin helpers (API.md §6): organizations, service accounts, passcode.

import { Secret, ValidationError } from "./core/index.js";
import {
  type CallOptions,
  type ClientContext,
  arr,
  microsToDate,
  num,
  obj,
  orgPath,
  requestJson,
  str,
} from "./http.js";

/** An organization. */
export interface Organization {
  id: number;
  identifier: string;
  name: string;
  /** `default` or `custom`. */
  type: string;
  userEmail?: string;
}

/** A service account (the server only lists masked tokens, so none is returned). */
export interface ServiceAccount {
  email: string;
  firstName: string;
  lastName: string;
  role: string;
  createdAt: Date | null;
}

/** A service-account credential: use `user` + `token` as `OPENOBSERVE_USER`/`OPENOBSERVE_TOKEN`. */
export interface ServiceAccountToken {
  user: string;
  /** The usable token (only returned on create and rotate). */
  token: Secret;
}

/** An ingest-only credential (Basic `user:passcode` can ingest but not query). */
export interface Passcode {
  user: string;
  passcode: Secret;
}

/** Options of `createServiceAccount`. */
export interface CreateServiceAccountOptions extends CallOptions {
  firstName?: string;
  lastName?: string;
}

/** Admin helpers: `client.admin`. OSS has no RBAC; every service account can administer. */
export class OpenObserveAdmin {
  readonly #ctx: ClientContext;

  /** @internal Use `client.admin`. */
  constructor(ctx: ClientContext) {
    this.#ctx = ctx;
  }

  /** Organizations visible to the user (`GET /api/organizations`, includes `_meta`). */
  async listOrganizations(opts: CallOptions = {}): Promise<Organization[]> {
    const res = await requestJson(this.#ctx, {
      path: "/api/organizations",
      operation: "admin.listOrganizations",
      ...signal(opts),
    });
    return arr(obj(res.data).data).map((raw) => {
      const o = obj(raw);
      const org: Organization = {
        id: num(o.id),
        identifier: str(o.identifier) ?? "",
        name: str(o.name) ?? "",
        type: str(o.type) ?? "",
      };
      const email = str(o.user_email);
      if (email !== undefined) org.userEmail = email;
      return org;
    });
  }

  /**
   * Creates a service account and returns its token (the only time the server shows it).
   * `ConflictError` when it exists, `ValidationError` for an invalid e-mail.
   */
  async createServiceAccount(
    email: string,
    opts: CreateServiceAccountOptions = {},
  ): Promise<ServiceAccountToken> {
    const res = await requestJson(this.#ctx, {
      method: "POST",
      path: orgPath(this.#ctx, "service_accounts"),
      json: {
        email: checkEmail(email, "admin.createServiceAccount"),
        first_name: opts.firstName ?? "",
        last_name: opts.lastName ?? "",
      },
      operation: "admin.createServiceAccount",
      ...signal(opts),
    });
    return tokenOf(obj(res.data), email);
  }

  /** Lists service accounts of the organization. */
  async listServiceAccounts(opts: CallOptions = {}): Promise<ServiceAccount[]> {
    const res = await requestJson(this.#ctx, {
      path: orgPath(this.#ctx, "service_accounts"),
      operation: "admin.listServiceAccounts",
      ...signal(opts),
    });
    return arr(obj(res.data).data).map((raw) => {
      const a = obj(raw);
      const created = num(a.created_at);
      return {
        email: str(a.email) ?? "",
        firstName: str(a.first_name) ?? "",
        lastName: str(a.last_name) ?? "",
        role: str(a.role) ?? "",
        createdAt: created > 0 ? microsToDate(created) : null,
      };
    });
  }

  /** Issues a new token for a service account; the old one stops working at once. */
  async rotateServiceAccountToken(
    email: string,
    opts: CallOptions = {},
  ): Promise<ServiceAccountToken> {
    const res = await requestJson(this.#ctx, {
      method: "PUT",
      path: orgPath(this.#ctx, "service_accounts", checkEmail(email, "admin.rotate")),
      query: { rotateToken: true },
      json: {},
      operation: "admin.rotateServiceAccountToken",
      ...signal(opts),
    });
    return tokenOf(obj(res.data), email);
  }

  /** Removes a service account; `NotFoundError` when missing. */
  async deleteServiceAccount(email: string, opts: CallOptions = {}): Promise<void> {
    await requestJson(this.#ctx, {
      method: "DELETE",
      path: orgPath(this.#ctx, "service_accounts", checkEmail(email, "admin.delete")),
      operation: "admin.deleteServiceAccount",
      ...signal(opts),
    });
  }

  /** The ingest-only passcode of the current user. */
  async getPasscode(opts: CallOptions = {}): Promise<Passcode> {
    const res = await requestJson(this.#ctx, {
      path: orgPath(this.#ctx, "passcode"),
      operation: "admin.getPasscode",
      ...signal(opts),
    });
    const data = obj(obj(res.data).data);
    const passcode = str(data.passcode);
    if (passcode === undefined) {
      throw new ValidationError("admin.getPasscode: the response has no passcode");
    }
    return { user: str(data.user) ?? "", passcode: new Secret(passcode) };
  }
}

function signal(opts: CallOptions): { signal?: AbortSignal } {
  return opts.signal === undefined ? {} : { signal: opts.signal };
}

function checkEmail(email: string, operation: string): string {
  const trimmed = email.trim();
  if (trimmed === "") throw new ValidationError(`${operation}: email must not be empty`);
  return trimmed;
}

function tokenOf(data: Record<string, unknown>, email: string): ServiceAccountToken {
  const token = str(data.token);
  if (token === undefined) {
    throw new ValidationError("admin.serviceAccount: the response has no token");
  }
  return { user: str(data.user) ?? email.trim(), token: new Secret(token) };
}
