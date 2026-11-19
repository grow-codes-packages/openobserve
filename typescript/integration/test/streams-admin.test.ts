// Streams and admin modules against the real service (write → read back).

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import {
  AuthenticationError,
  ConflictError,
  NotFoundError,
  OpenObserveClient,
} from "@grow-codes/openobserve";
import { eventually, rootClient, suffix, uniqueStream } from "./helpers.ts";

let client: OpenObserveClient;
before(() => {
  client = rootClient();
});
after(async () => {
  await client.close();
});

test("createStream → listStreams → getStreamSchema → updateStreamSettings → deleteStream", async () => {
  const stream = uniqueStream("streams");
  await client.streams.createStream(stream, {
    fields: [{ name: "message", type: "Utf8" }],
    retentionDays: 3,
  });
  await client.logs.ingestJson(stream, [{ message: "s1", run_id: suffix }]);
  const listed = await eventually(
    () => client.streams.listStreams("logs", { keyword: stream }),
    (l) => l.some((s) => s.name === stream),
  );
  assert.ok(listed.some((s) => s.name === stream && s.streamType === "logs"));

  const schema = await client.streams.getStreamSchema(stream);
  assert.ok(schema.schema.some((f) => f.name === "message"));
  assert.ok(schema.schema.some((f) => f.name === "run_id"));
  assert.equal(schema.settings.retentionDays, 3);

  await client.streams.updateStreamSettings(stream, {
    retentionDays: 7,
    fullTextSearchKeys: ["message"],
    partitionKeys: ["run_id"],
  });
  const after1 = await client.streams.getStreamSchema(stream);
  assert.equal(after1.settings.retentionDays, 7);
  assert.deepEqual(after1.settings.fullTextSearchKeys, ["message"]);
  assert.ok(after1.settings.partitionKeys.some((k) => k.field === "run_id" && !k.disabled));

  await client.streams.updateStreamSettings(stream, {
    fullTextSearchKeys: { remove: ["message"] },
    partitionKeys: [],
  });
  const after2 = await client.streams.getStreamSchema(stream);
  assert.deepEqual(after2.settings.fullTextSearchKeys, []);
  assert.ok(after2.settings.partitionKeys.every((k) => k.disabled));

  await client.streams.deleteStream(stream);
  await assert.rejects(client.streams.getStreamSchema(stream), NotFoundError);
  await assert.rejects(client.streams.deleteStream(stream), NotFoundError);
});

test("the bootstrap stream is listed for traces and logs types", async () => {
  const logs = await client.streams.listStreams("logs");
  assert.ok(logs.some((s) => s.name === process.env.OPENOBSERVE_STREAM));
});

test("listOrganizations includes the configured org", async () => {
  const orgs = await client.admin.listOrganizations();
  assert.ok(orgs.some((o) => o.identifier === client.config.org));
});

test("service account lifecycle: create → use → list → rotate → delete", async () => {
  const email = `ts-sa-${suffix}@stack.local`;
  const created = await client.admin.createServiceAccount(email, {
    firstName: "TS",
    lastName: suffix,
  });
  assert.equal(created.user, email);
  assert.ok(!String(created.token).includes(created.token.reveal()));
  await assert.rejects(client.admin.createServiceAccount(email), ConflictError);

  const sa = OpenObserveClient.fromEnv({ user: email, token: created.token.reveal() });
  try {
    assert.ok((await sa.ping()) >= 0);
  } finally {
    await sa.close();
  }
  const listed = await client.admin.listServiceAccounts();
  assert.ok(listed.some((a) => a.email === email && a.firstName === "TS"));

  const rotated = await client.admin.rotateServiceAccountToken(email);
  assert.notEqual(rotated.token.reveal(), created.token.reveal());
  const oldClient = OpenObserveClient.fromEnv({ user: email, token: created.token.reveal() });
  const newClient = OpenObserveClient.fromEnv({ user: email, token: rotated.token.reveal() });
  try {
    await assert.rejects(oldClient.ping(), AuthenticationError);
    assert.ok((await newClient.ping()) >= 0);
  } finally {
    await oldClient.close();
    await newClient.close();
  }

  await client.admin.deleteServiceAccount(email);
  assert.ok(!(await client.admin.listServiceAccounts()).some((a) => a.email === email));
  await assert.rejects(client.admin.deleteServiceAccount(email), NotFoundError);
});

test("getPasscode gives an ingest-only credential", async () => {
  const pc = await client.admin.getPasscode();
  assert.equal(pc.user, client.config.user);
  const stream = uniqueStream("passcode");
  const ingest = OpenObserveClient.fromEnv({ user: pc.user, password: pc.passcode.reveal() });
  try {
    const res = await ingest.logs.ingestJson(stream, [{ message: "via passcode" }]);
    assert.equal(res.successful, 1);
    await assert.rejects(ingest.logs.search(`SELECT * FROM "${stream}"`), AuthenticationError);
  } finally {
    await ingest.close();
  }
  const hits = await eventually(
    () => client.logs.search(`SELECT * FROM "${stream}"`),
    (r) => r.hits.length >= 1,
  );
  assert.equal(hits.hits[0]!.message, "via passcode");
  await client.streams.deleteStream(stream);
});
