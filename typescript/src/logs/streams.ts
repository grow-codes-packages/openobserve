// Service-owned. Stream management (API.md §4): list, schema, settings, create, delete.

import { ValidationError } from "../core/index.js";
import {
  type CallOptions,
  type ClientContext,
  type JsonObject,
  type StreamType,
  arr,
  microsToDate,
  num,
  obj,
  orgPath,
  requestJson,
  str,
  streamName,
} from "../http.js";

/** Options with a stream type (default `logs`). */
export interface StreamCallOptions extends CallOptions {
  type?: StreamType;
}

/** Options of `listStreams`. */
export interface ListStreamsOptions extends CallOptions {
  /** Only names containing this text. */
  keyword?: string;
  offset?: number;
  limit?: number;
}

/** One schema field. */
export interface SchemaField {
  name: string;
  /** Arrow type name: `Utf8`, `Int64`, `Float64`, `Boolean`, … */
  type: string;
}

/** A partition key. */
export interface PartitionKey {
  field: string;
  /** `value` (default) or `hash`. */
  types: string;
  /** Removed keys stay listed with `disabled: true`. */
  disabled: boolean;
}

/** Stream settings (selected fields; `raw` holds the full server object). */
export interface StreamSettings {
  partitionKeys: PartitionKey[];
  fullTextSearchKeys: string[];
  indexFields: string[];
  bloomFilterFields: string[];
  /** Retention in days; 0 = the server default. */
  retentionDays: number;
  raw: JsonObject;
}

/** Stream statistics. */
export interface StreamStats {
  createdAt: Date | null;
  docNum: number;
  storageSize: number;
  compressedSize: number;
}

/** A stream with its schema and settings. */
export interface StreamInfo {
  name: string;
  streamType: string;
  storageType: string;
  stats: StreamStats;
  schema: SchemaField[];
  settings: StreamSettings;
  totalFields: number;
}

/** A partition key in an update: a field name (type `value`) or `{ field, types }`. */
export type PartitionKeyInput = string | { field: string; types?: string };

/** A list setting: a full list (replace) or a delta. */
export type ListUpdate<T> = readonly T[] | { add?: readonly T[]; remove?: readonly T[] };

/** Settings to change. Omitted settings stay unchanged. */
export interface StreamSettingsUpdate {
  retentionDays?: number;
  /** A list replaces the enabled partition keys; `{ add, remove }` is applied as given. */
  partitionKeys?: ListUpdate<PartitionKeyInput>;
  fullTextSearchKeys?: ListUpdate<string>;
  indexFields?: ListUpdate<string>;
  bloomFilterFields?: ListUpdate<string>;
}

/** Options of `createStream`. */
export interface CreateStreamOptions extends StreamCallOptions {
  /** Initial schema. */
  fields?: readonly SchemaField[];
  retentionDays?: number;
  partitionKeys?: readonly PartitionKeyInput[];
  fullTextSearchKeys?: readonly string[];
  indexFields?: readonly string[];
  bloomFilterFields?: readonly string[];
}

/** The OpenObserve streams module: `client.streams`. */
export class OpenObserveStreams {
  readonly #ctx: ClientContext;

  /** @internal Use `client.streams`. */
  constructor(ctx: ClientContext) {
    this.#ctx = ctx;
  }

  /** Lists streams of one type: `GET /api/{org}/streams?type=…` (sorted by name). */
  async listStreams(
    type: StreamType = "logs",
    opts: ListStreamsOptions = {},
  ): Promise<StreamInfo[]> {
    const res = await requestJson(this.#ctx, {
      path: orgPath(this.#ctx, "streams"),
      query: {
        type,
        keyword: opts.keyword,
        offset: opts.offset,
        limit: opts.limit,
        sort: "name",
        asc: true,
      },
      operation: "streams.listStreams",
      ...signal(opts),
    });
    return arr(obj(res.data).list).map((s) => streamInfo(obj(s)));
  }

  /** Schema, stats and settings of one stream; `NotFoundError` when missing. */
  async getStreamSchema(stream: string, opts: StreamCallOptions = {}): Promise<StreamInfo> {
    const name = streamName(stream, "", "streams.getStreamSchema");
    const res = await requestJson(this.#ctx, {
      path: orgPath(this.#ctx, "streams", name, "schema"),
      query: { type: opts.type ?? "logs" },
      operation: `streams.getStreamSchema ${name}`,
      ...signal(opts),
    });
    return streamInfo(obj(res.data));
  }

  /**
   * Changes stream settings: `PUT /api/{org}/streams/{stream}/settings`. A list value replaces the
   * current list (the SDK reads the schema and sends add/remove deltas); `{ add, remove }` is sent
   * as given. `NotFoundError` when the stream is missing.
   */
  async updateStreamSettings(
    stream: string,
    update: StreamSettingsUpdate,
    opts: StreamCallOptions = {},
  ): Promise<void> {
    const name = streamName(stream, "", "streams.updateStreamSettings");
    const type = opts.type ?? "logs";
    const needsCurrent = [
      update.partitionKeys,
      update.fullTextSearchKeys,
      update.indexFields,
      update.bloomFilterFields,
    ].some((u) => Array.isArray(u));
    const current = needsCurrent
      ? (await this.getStreamSchema(name, { ...opts, type })).settings
      : undefined;
    const body: JsonObject = {};
    if (update.retentionDays !== undefined) {
      if (!Number.isInteger(update.retentionDays) || update.retentionDays < 0) {
        throw new ValidationError("streams.updateStreamSettings: retentionDays must be >= 0");
      }
      body.data_retention = update.retentionDays;
    }
    if (update.partitionKeys !== undefined) {
      const enabled = (current?.partitionKeys ?? [])
        .filter((k) => !k.disabled)
        .map((k) => ({ field: k.field, types: k.types }));
      body.partition_keys = delta(
        update.partitionKeys,
        enabled,
        (k) => (typeof k === "string" ? k : k.field),
        (k) => (typeof k === "string" ? { field: k, types: "value" } : { types: "value", ...k }),
      );
    }
    const lists: [keyof StreamSettingsUpdate, keyof StreamSettings, string][] = [
      ["fullTextSearchKeys", "fullTextSearchKeys", "full_text_search_keys"],
      ["indexFields", "indexFields", "index_fields"],
      ["bloomFilterFields", "bloomFilterFields", "bloom_filter_fields"],
    ];
    for (const [key, currentKey, wire] of lists) {
      const value = update[key] as ListUpdate<string> | undefined;
      if (value === undefined) continue;
      body[wire] = delta(
        value,
        (current?.[currentKey] as string[] | undefined) ?? [],
        (s) => s,
        (s) => s,
      );
    }
    await requestJson(this.#ctx, {
      method: "PUT",
      path: orgPath(this.#ctx, "streams", name, "settings"),
      query: { type },
      json: body,
      operation: `streams.updateStreamSettings ${name}`,
      ...signal(opts),
    });
  }

  /** Creates a stream with an optional schema and settings: `POST /api/{org}/streams/{stream}`. */
  async createStream(stream: string, opts: CreateStreamOptions = {}): Promise<void> {
    const name = streamName(stream, "", "streams.createStream");
    await requestJson(this.#ctx, {
      method: "POST",
      path: orgPath(this.#ctx, "streams", name),
      query: { type: opts.type ?? "logs" },
      json: {
        fields: (opts.fields ?? []).map((f) => ({ name: f.name, type: f.type })),
        settings: {
          partition_keys: (opts.partitionKeys ?? []).map((k) =>
            typeof k === "string" ? { field: k, types: "value" } : { types: "value", ...k },
          ),
          index_fields: opts.indexFields ?? [],
          full_text_search_keys: opts.fullTextSearchKeys ?? [],
          bloom_filter_fields: opts.bloomFilterFields ?? [],
          data_retention: opts.retentionDays ?? 0,
        },
      },
      operation: `streams.createStream ${name}`,
      ...signal(opts),
    });
  }

  /**
   * Deletes a stream and its data: `DELETE /api/{org}/streams/{stream}` (alerts and dashboards
   * are kept). `NotFoundError` when missing.
   */
  async deleteStream(stream: string, opts: StreamCallOptions = {}): Promise<void> {
    const name = streamName(stream, "", "streams.deleteStream");
    await requestJson(this.#ctx, {
      method: "DELETE",
      path: orgPath(this.#ctx, "streams", name),
      query: { type: opts.type ?? "logs" },
      operation: `streams.deleteStream ${name}`,
      ...signal(opts),
    });
  }
}

function signal(opts: CallOptions): { signal?: AbortSignal } {
  return opts.signal === undefined ? {} : { signal: opts.signal };
}

function delta<In, Out>(
  update: ListUpdate<In>,
  current: readonly Out[],
  keyOf: (v: In | Out) => string,
  toWire: (v: In) => Out,
): { add: Out[]; remove: Out[] } {
  if (!Array.isArray(update)) {
    const d = update as { add?: readonly In[]; remove?: readonly In[] };
    return { add: (d.add ?? []).map(toWire), remove: (d.remove ?? []).map(toWire) };
  }
  const wanted = (update as readonly In[]).map(toWire);
  const wantedKeys = new Set(wanted.map((w) => keyOf(w)));
  const currentKeys = new Set(current.map((c) => keyOf(c)));
  return {
    add: wanted.filter((w) => !currentKeys.has(keyOf(w))),
    remove: current.filter((c) => !wantedKeys.has(keyOf(c))),
  };
}

function strings(value: unknown): string[] {
  return arr(value).filter((v): v is string => typeof v === "string");
}

function streamInfo(data: JsonObject): StreamInfo {
  const stats = obj(data.stats);
  const settings = obj(data.settings);
  const created = num(stats.created_at);
  return {
    name: str(data.name) ?? "",
    streamType: str(data.stream_type) ?? "",
    storageType: str(data.storage_type) ?? "",
    stats: {
      createdAt: created > 0 ? microsToDate(created) : null,
      docNum: num(stats.doc_num),
      storageSize: num(stats.storage_size),
      compressedSize: num(stats.compressed_size),
    },
    schema: arr(data.schema).map((f) => {
      const field = obj(f);
      return { name: str(field.name) ?? "", type: str(field.type) ?? "" };
    }),
    settings: {
      partitionKeys: Object.values(obj(settings.partition_keys)).map((k) => {
        const key = obj(k);
        return {
          field: str(key.field) ?? "",
          types: typeof key.types === "string" ? key.types : JSON.stringify(key.types ?? "value"),
          disabled: key.disabled === true,
        };
      }),
      fullTextSearchKeys: strings(settings.full_text_search_keys),
      indexFields: strings(settings.index_fields),
      bloomFilterFields: strings(settings.bloom_filter_fields),
      retentionDays: num(settings.data_retention),
      raw: settings,
    },
    totalFields: num(data.total_fields),
  };
}
