import { inspect } from "node:util";
import { afterEach, describe, expect, it } from "vitest";
import { ConflictError, NotFoundError, OpenObserveClient, ValidationError } from "../src/index.js";
import { fx } from "./helpers/fixtures.js";
import {
  type MockHandler,
  type MockResponse,
  type MockServer,
  startServer,
} from "./helpers/server.js";

const servers: MockServer[] = [];
const clients: OpenObserveClient[] = [];
afterEach(async () => {
  await Promise.all(clients.splice(0).map((c) => c.close()));
  await Promise.all(servers.splice(0).map((s) => s.close()));
});

async function setup(
  handler: MockHandler | MockResponse[],
): Promise<{ s: MockServer; client: OpenObserveClient }> {
  const s = await startServer(handler);
  servers.push(s);
  const client = new OpenObserveClient({
    url: s.url,
    user: "admin@stack.local",
    password: "Gw1-secret-pass",
    retry: { maxRetries: 0 },
  });
  clients.push(client);
  return { s, client };
}

const body = (s: MockServer, i = 0): any => JSON.parse(s.requests[i]!.body);

describe("streams", () => {
  it("listStreams sends type, paging and maps stream info", async () => {
    const { s, client } = await setup([fx("listStreams.logs")]);
    const list = await client.streams.listStreams("logs", { keyword: "sdk", offset: 0, limit: 5 });
    const url = new URL(s.requests[0]!.url, "http://x");
    expect(url.pathname).toBe("/api/default/streams");
    expect(Object.fromEntries(url.searchParams)).toEqual({
      type: "logs",
      keyword: "sdk",
      offset: "0",
      limit: "5",
      sort: "name",
      asc: "true",
    });
    expect(list.length).toBeGreaterThan(0);
    expect(list[0]).toMatchObject({
      name: "passcode_6moiazbq",
      streamType: "logs",
      storageType: "disk",
    });
    expect(list[0]!.stats.createdAt).toBeInstanceOf(Date);
    expect(list[0]!.settings.retentionDays).toBe(0);
  });

  it("listStreams defaults to logs", async () => {
    const { s, client } = await setup([fx("listStreams.traces")]);
    await client.streams.listStreams();
    expect(s.requests[0]!.url).toContain("type=logs");
  });

  it("getStreamSchema maps schema, settings and partition keys", async () => {
    const { s, client } = await setup([
      fx("getStreamSchema.ok"),
      fx("getStreamSchema.after_settings"),
    ]);
    const info = await client.streams.getStreamSchema("sdk_logs_6moiazbq");
    expect(s.requests[0]!.url).toBe("/api/default/streams/sdk_logs_6moiazbq/schema?type=logs");
    expect(info.schema).toContainEqual({ name: "message", type: "Utf8" });
    expect(info.totalFields).toBe(8);
    const after = await client.streams.getStreamSchema("sdk_logs_6moiazbq", { type: "logs" });
    expect(after.settings.partitionKeys).toEqual([
      { field: "run_id", types: "value", disabled: true },
    ]);
    expect(after.settings.retentionDays).toBe(7);
  });

  it("maps odd stream payloads", async () => {
    const { client } = await setup([
      {
        status: 200,
        body: {
          schema: [{}],
          settings: { partition_keys: { L0: { types: { hash: 8 } } } },
          stats: {},
        },
      },
    ]);
    const info = await client.streams.getStreamSchema("s");
    expect(info.name).toBe("");
    expect(info.stats.createdAt).toBeNull();
    expect(info.schema).toEqual([{ name: "", type: "" }]);
    expect(info.settings.partitionKeys[0]).toEqual({
      field: "",
      types: '{"hash":8}',
      disabled: false,
    });
  });

  it("getStreamSchema maps 404", async () => {
    const { client } = await setup([fx("getStreamSchema.404")]);
    await expect(client.streams.getStreamSchema("nostream")).rejects.toBeInstanceOf(NotFoundError);
  });

  it("updateStreamSettings sends deltas as given", async () => {
    const { s, client } = await setup([fx("updateStreamSettings.ok")]);
    await client.streams.updateStreamSettings("sdk_logs_6moiazbq", {
      retentionDays: 7,
      fullTextSearchKeys: { add: ["message"] },
      partitionKeys: { add: ["run_id"], remove: [{ field: "x", types: "hash" }] },
      indexFields: { remove: ["a"] },
      bloomFilterFields: { add: ["b"] },
    });
    expect(s.requests[0]!.method).toBe("PUT");
    expect(s.requests[0]!.url).toBe("/api/default/streams/sdk_logs_6moiazbq/settings?type=logs");
    expect(body(s)).toEqual({
      data_retention: 7,
      partition_keys: {
        add: [{ field: "run_id", types: "value" }],
        remove: [{ field: "x", types: "hash" }],
      },
      full_text_search_keys: { add: ["message"], remove: [] },
      index_fields: { add: [], remove: ["a"] },
      bloom_filter_fields: { add: ["b"], remove: [] },
    });
  });

  it("updateStreamSettings turns lists into deltas from the current settings", async () => {
    const current = fx("getStreamSchema.ok", (b) => {
      b.settings.partition_keys = {
        L0: { field: "old", types: "value", disabled: false },
        L1: { field: "gone", types: "value", disabled: true },
      };
      b.settings.full_text_search_keys = ["message", "log"];
      return b;
    });
    const { s, client } = await setup([current, fx("updateStreamSettings.ok")]);
    await client.streams.updateStreamSettings("s", {
      partitionKeys: ["run_id", "old"],
      fullTextSearchKeys: ["message", "body"],
    });
    expect(s.requests[0]!.method).toBe("GET");
    expect(body(s, 1)).toEqual({
      partition_keys: { add: [{ field: "run_id", types: "value" }], remove: [] },
      full_text_search_keys: { add: ["body"], remove: ["log"] },
    });
  });

  it("updateStreamSettings validates and maps errors", async () => {
    const { client } = await setup([
      fx("updateStreamSettings.404"),
      fx("updateStreamSettings.422_plain_array"),
    ]);
    await expect(client.streams.updateStreamSettings("s", { retentionDays: -1 })).rejects.toThrow(
      /retentionDays/,
    );
    await expect(
      client.streams.updateStreamSettings("s", { retentionDays: 1 }),
    ).rejects.toBeInstanceOf(NotFoundError);
    await expect(client.streams.updateStreamSettings("s", {})).rejects.toBeInstanceOf(
      ValidationError,
    );
  });

  it("createStream sends plain arrays", async () => {
    const { s, client } = await setup([fx("createStream.ok")]);
    await client.streams.createStream("New.S", {
      fields: [{ name: "message", type: "Utf8" }],
      retentionDays: 3,
      partitionKeys: ["k", { field: "h", types: "hash" }],
      fullTextSearchKeys: ["message"],
      indexFields: ["i"],
      bloomFilterFields: ["b"],
    });
    expect(s.requests[0]!.url).toBe("/api/default/streams/new_s?type=logs");
    expect(body(s)).toEqual({
      fields: [{ name: "message", type: "Utf8" }],
      settings: {
        partition_keys: [
          { field: "k", types: "value" },
          { field: "h", types: "hash" },
        ],
        index_fields: ["i"],
        full_text_search_keys: ["message"],
        bloom_filter_fields: ["b"],
        data_retention: 3,
      },
    });
    await client.streams.createStream("t", { type: "traces" });
    expect(body(s, 1).settings.data_retention).toBe(0);
  });

  it("deleteStream and its 404", async () => {
    const { s, client } = await setup([fx("deleteStream.ok"), fx("deleteStream.404")]);
    await client.streams.deleteStream("sdk_bulk_6moiazbq");
    expect(s.requests[0]!.method).toBe("DELETE");
    expect(s.requests[0]!.url).toBe("/api/default/streams/sdk_bulk_6moiazbq?type=logs");
    await expect(client.streams.deleteStream("sdk_bulk_6moiazbq")).rejects.toBeInstanceOf(
      NotFoundError,
    );
  });
});

describe("admin", () => {
  it("listOrganizations uses /api/organizations", async () => {
    const { s, client } = await setup([
      fx("listOrganizations.ok", (b) => {
        b.data.push({});
        return b;
      }),
    ]);
    const orgs = await client.admin.listOrganizations();
    expect(s.requests[0]!.url).toBe("/api/organizations");
    expect(orgs[1]).toEqual({
      id: 1,
      identifier: "default",
      name: "default",
      type: "default",
      userEmail: "admin@stack.local",
    });
    expect(orgs[2]).toEqual({ id: 0, identifier: "", name: "", type: "" });
  });

  it("createServiceAccount returns a redacting token", async () => {
    const { s, client } = await setup([
      fx("createServiceAccount.ok", (b) => ({ ...b, token: "tok-123456789" })),
    ]);
    const sa = await client.admin.createServiceAccount(" sdk@stack.local ", {
      firstName: "SDK",
      lastName: "T",
    });
    expect(body(s)).toEqual({ email: "sdk@stack.local", first_name: "SDK", last_name: "T" });
    expect(sa.user).toBe("sdk-6moiazbq@stack.local");
    expect(sa.token.reveal()).toBe("tok-123456789");
    expect(inspect(sa)).not.toContain("tok-123456789");
    expect(JSON.stringify(sa)).not.toContain("tok-123456789");
  });

  it("createServiceAccount maps duplicate and invalid e-mail", async () => {
    const { client } = await setup([
      fx("createServiceAccount.400_duplicate"),
      fx("createServiceAccount.400_invalid_email"),
    ]);
    await expect(client.admin.createServiceAccount("a@b.c")).rejects.toBeInstanceOf(ConflictError);
    await expect(client.admin.createServiceAccount("nope")).rejects.toBeInstanceOf(ValidationError);
    await expect(client.admin.createServiceAccount(" ")).rejects.toThrow(/email/);
  });

  it("createServiceAccount without a token in the response", async () => {
    const { client } = await setup([{ status: 200, body: { code: 200 } }]);
    await expect(client.admin.createServiceAccount("a@b.c")).rejects.toThrow(/no token/);
  });

  it("listServiceAccounts drops the masked tokens", async () => {
    const { client } = await setup([
      fx("listServiceAccounts.ok", (b) => {
        b.data.push({});
        return b;
      }),
    ]);
    const list = await client.admin.listServiceAccounts();
    expect(list[0]).toMatchObject({ email: "sdk-6moiazbq@stack.local", role: "service_account" });
    expect(list[0]!.createdAt).toBeInstanceOf(Date);
    expect(list[0]).not.toHaveProperty("token");
    expect(list[2]!.createdAt).toBeNull();
  });

  it("rotateServiceAccountToken PUTs rotateToken=true with an encoded e-mail", async () => {
    const { s, client } = await setup([
      fx("rotateServiceAccountToken.ok", () => ({ token: "new-token-1", user: undefined })),
    ]);
    const sa = await client.admin.rotateServiceAccountToken("sdk-6moiazbq@stack.local");
    expect(s.requests[0]!.url).toBe(
      "/api/default/service_accounts/sdk-6moiazbq%40stack.local?rotateToken=true",
    );
    expect(body(s)).toEqual({});
    expect(sa).toMatchObject({ user: "sdk-6moiazbq@stack.local" });
    expect(sa.token.reveal()).toBe("new-token-1");
  });

  it("deleteServiceAccount and its 404", async () => {
    const { s, client } = await setup([
      fx("deleteServiceAccount.ok"),
      fx("deleteServiceAccount.404"),
    ]);
    await client.admin.deleteServiceAccount("a@b.c");
    expect(s.requests[0]!.method).toBe("DELETE");
    await expect(client.admin.deleteServiceAccount("a@b.c")).rejects.toBeInstanceOf(NotFoundError);
  });

  it("getPasscode wraps the passcode", async () => {
    const { client } = await setup([
      fx("getPasscode.ok", () => ({ data: { passcode: "pc-123456", user: "admin@stack.local" } })),
      { status: 200, body: { data: {} } },
    ]);
    const pc = await client.admin.getPasscode();
    expect(pc.user).toBe("admin@stack.local");
    expect(pc.passcode.reveal()).toBe("pc-123456");
    expect(String(pc.passcode)).not.toContain("pc-123456");
    await expect(client.admin.getPasscode()).rejects.toThrow(/no passcode/);
  });
});
