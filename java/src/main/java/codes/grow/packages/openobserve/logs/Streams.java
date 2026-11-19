package codes.grow.packages.openobserve.logs;

import codes.grow.packages.openobserve.core.TransportRequest;
import codes.grow.packages.openobserve.core.ValidationException;
import codes.grow.packages.openobserve.internal.Api;
import codes.grow.packages.openobserve.internal.Values;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * Stream management of OpenObserve ({@code OpenObserveClient.streams()}). Every method has a
 * blocking form and an {@code …Async} form.
 */
public final class Streams {
  private final Api api;

  /**
   * Creates the module (use {@code OpenObserveClient.streams()}).
   *
   * @param api request helper
   */
  public Streams(Api api) {
    this.api = api;
  }

  /**
   * Lists every log stream.
   *
   * @return streams
   */
  public StreamList listStreams() {
    return listStreams(ListStreamsOptions.of(StreamType.LOGS));
  }

  /**
   * Lists every stream of a type.
   *
   * @param type stream type
   * @return streams
   */
  public StreamList listStreams(StreamType type) {
    return listStreams(ListStreamsOptions.of(type));
  }

  /**
   * Lists streams of one type ({@code GET /api/{org}/streams}).
   *
   * @param options type, keyword and paging
   * @return one page of streams with the total number of matches
   */
  public StreamList listStreams(ListStreamsOptions options) {
    return Api.await(listStreamsAsync(options), "streams.listStreams");
  }

  /**
   * Asynchronous {@link #listStreams(ListStreamsOptions)}.
   *
   * @param options options
   * @return future streams
   */
  public CompletableFuture<StreamList> listStreamsAsync(ListStreamsOptions options) {
    ListStreamsOptions o = options == null ? ListStreamsOptions.of(null) : options;
    TransportRequest.Builder req =
        TransportRequest.get(api.orgPath("/streams"))
            .query("type", o.type().wireValue())
            .operation("streams.listStreams");
    if (o.keyword() != null && !o.keyword().isEmpty()) {
      req.query("keyword", o.keyword());
    }
    if (o.offset() > 0) {
      req.query("offset", o.offset());
    }
    if (o.limit() > 0) {
      req.query("limit", o.limit());
    }
    return api.jsonAsync(req)
        .thenApply(
            body -> {
              List<StreamInfo> out = new ArrayList<>();
              for (JsonNode s : body.path("list")) {
                out.add(info(s));
              }
              return new StreamList(out, body.path("total").asInt(out.size()));
            });
  }

  /**
   * Returns the schema, settings and statistics of a log stream.
   *
   * @param stream stream name
   * @return the stream
   * @throws codes.grow.packages.openobserve.core.NotFoundException when it does not exist
   */
  public StreamInfo getStreamSchema(String stream) {
    return getStreamSchema(stream, StreamType.LOGS);
  }

  /**
   * Returns the schema, settings and statistics of a stream ({@code GET
   * /api/{org}/streams/{stream}/schema}).
   *
   * @param stream stream name
   * @param type stream type (null = logs)
   * @return the stream
   * @throws codes.grow.packages.openobserve.core.NotFoundException when the stream (of that type)
   *     does not exist
   */
  public StreamInfo getStreamSchema(String stream, StreamType type) {
    return Api.await(getStreamSchemaAsync(stream, type), "streams.getStreamSchema");
  }

  /**
   * Asynchronous {@link #getStreamSchema(String, StreamType)}.
   *
   * @param stream stream name
   * @param type stream type
   * @return future stream
   */
  public CompletableFuture<StreamInfo> getStreamSchemaAsync(String stream, StreamType type) {
    String op = "streams.getStreamSchema";
    return Api.defer(
        () ->
            api.jsonAsync(
                    TransportRequest.get(
                            api.orgPath("/streams/" + Values.stream(op, stream) + "/schema"))
                        .query("type", StreamType.orLogs(type).wireValue())
                        .operation(op))
                .thenApply(Streams::info));
  }

  /**
   * Applies settings changes ({@code PUT /api/{org}/streams/{stream}/settings}). List changes read
   * the current schema first and send add/remove deltas. Nothing is sent when nothing changes.
   *
   * @param stream stream name
   * @param update changes
   */
  public void updateStreamSettings(String stream, StreamSettingsUpdate update) {
    Api.await(updateStreamSettingsAsync(stream, update), "streams.updateStreamSettings");
  }

  /**
   * Asynchronous {@link #updateStreamSettings}.
   *
   * @param stream stream name
   * @param update changes
   * @return completes when applied
   */
  public CompletableFuture<Void> updateStreamSettingsAsync(
      String stream, StreamSettingsUpdate update) {
    String op = "streams.updateStreamSettings";
    return Api.defer(
        () -> {
          String name = Values.stream(op, stream);
          if (update == null) {
            throw new ValidationException(op + ": update is required");
          }
          if (update.retentionDays().isPresent() && update.retentionDays().getAsInt() < 0) {
            throw new ValidationException(op + ": retentionDays must not be negative");
          }
          Map<String, Object> body = new LinkedHashMap<>();
          update.retentionDays().ifPresent(d -> body.put("data_retention", d));
          CompletableFuture<Map<String, Object>> ready;
          if (update.partitionKeys().isPresent()
              || update.fullTextSearchKeys().isPresent()
              || update.indexFields().isPresent()
              || update.bloomFilterFields().isPresent()) {
            ready =
                getStreamSchemaAsync(name, update.type())
                    .thenApply(
                        current -> {
                          update
                              .fullTextSearchKeys()
                              .map(
                                  want ->
                                      stringDelta(current.settings().fullTextSearchKeys(), want))
                              .ifPresent(d -> body.put("full_text_search_keys", d));
                          update
                              .indexFields()
                              .map(want -> stringDelta(current.settings().indexFields(), want))
                              .ifPresent(d -> body.put("index_fields", d));
                          update
                              .bloomFilterFields()
                              .map(
                                  want -> stringDelta(current.settings().bloomFilterFields(), want))
                              .ifPresent(d -> body.put("bloom_filter_fields", d));
                          update
                              .partitionKeys()
                              .map(want -> partitionDelta(current.settings().partitionKeys(), want))
                              .ifPresent(d -> body.put("partition_keys", d));
                          return body;
                        });
          } else {
            ready = CompletableFuture.completedFuture(body);
          }
          return ready.thenCompose(
              b -> {
                if (b.isEmpty()) {
                  return CompletableFuture.<Void>completedFuture(null);
                }
                return api.sendAsync(
                        TransportRequest.put(api.orgPath("/streams/" + name + "/settings"))
                            .query("type", update.type().wireValue())
                            .json(b)
                            .operation(op))
                    .thenApply(r -> (Void) null);
              });
        });
  }

  /**
   * Returns the add/remove delta from {@code current} to {@code want}, or null when equal.
   *
   * @param current current list
   * @param want wanted list
   * @return delta object or null
   */
  static Map<String, Object> stringDelta(List<String> current, List<String> want) {
    Set<String> have = new LinkedHashSet<>(current);
    Set<String> wanted = new LinkedHashSet<>(want);
    List<String> add = new ArrayList<>();
    for (String w : wanted) {
      if (!have.contains(w)) {
        add.add(w);
      }
    }
    List<String> remove = new ArrayList<>();
    for (String c : have) {
      if (!wanted.contains(c)) {
        remove.add(c);
      }
    }
    if (add.isEmpty() && remove.isEmpty()) {
      return null;
    }
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("add", add);
    out.put("remove", remove);
    return out;
  }

  /**
   * Returns the partition key delta from the enabled {@code current} keys to {@code want}, or null.
   *
   * @param current current keys (disabled ones ignored)
   * @param want wanted keys
   * @return delta object or null
   */
  static Map<String, Object> partitionDelta(List<PartitionKey> current, List<PartitionKey> want) {
    Map<String, PartitionKey> enabled = new LinkedHashMap<>();
    for (PartitionKey c : current) {
      if (!c.disabled()) {
        enabled.put(c.field(), c);
      }
    }
    Set<String> wanted = new LinkedHashSet<>();
    List<Map<String, String>> add = new ArrayList<>();
    for (PartitionKey w : want) {
      if (wanted.add(w.field()) && !enabled.containsKey(w.field())) {
        add.add(Map.of("field", w.field(), "types", w.types()));
      }
    }
    List<Map<String, String>> remove = new ArrayList<>();
    for (PartitionKey c : enabled.values()) {
      if (!wanted.contains(c.field())) {
        remove.add(Map.of("field", c.field(), "types", c.types()));
      }
    }
    if (add.isEmpty() && remove.isEmpty()) {
      return null;
    }
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("add", add);
    out.put("remove", remove);
    return out;
  }

  /**
   * Deletes a log stream and its data.
   *
   * @param stream stream name
   */
  public void deleteStream(String stream) {
    deleteStream(stream, StreamType.LOGS);
  }

  /**
   * Deletes a stream and its data ({@code DELETE /api/{org}/streams/{stream}}). Related alerts and
   * dashboards are kept.
   *
   * @param stream stream name
   * @param type stream type (null = logs)
   * @throws codes.grow.packages.openobserve.core.NotFoundException when it does not exist
   */
  public void deleteStream(String stream, StreamType type) {
    Api.await(deleteStreamAsync(stream, type), "streams.deleteStream");
  }

  /**
   * Asynchronous {@link #deleteStream(String, StreamType)}.
   *
   * @param stream stream name
   * @param type stream type
   * @return completes when deleted
   */
  public CompletableFuture<Void> deleteStreamAsync(String stream, StreamType type) {
    String op = "streams.deleteStream";
    return Api.defer(
        () ->
            api.sendAsync(
                    TransportRequest.delete(api.orgPath("/streams/" + Values.stream(op, stream)))
                        .query("type", StreamType.orLogs(type).wireValue())
                        .operation(op))
                .thenApply(r -> (Void) null));
  }

  /**
   * Creates a stream ahead of ingestion ({@code POST /api/{org}/streams/{stream}}).
   *
   * @param stream stream name
   * @param options type, fields and initial settings
   */
  public void createStream(String stream, CreateStreamOptions options) {
    Api.await(createStreamAsync(stream, options), "streams.createStream");
  }

  /**
   * Asynchronous {@link #createStream}.
   *
   * @param stream stream name
   * @param options options
   * @return completes when created
   */
  public CompletableFuture<Void> createStreamAsync(String stream, CreateStreamOptions options) {
    String op = "streams.createStream";
    return Api.defer(
        () -> {
          String name = Values.stream(op, stream);
          CreateStreamOptions o = options == null ? CreateStreamOptions.defaults() : options;
          List<Map<String, String>> fields = new ArrayList<>();
          for (SchemaField f : o.fields()) {
            fields.add(Map.of("name", f.name(), "type", f.type()));
          }
          List<Map<String, String>> pks = new ArrayList<>();
          for (PartitionKey pk : o.settings().partitionKeys()) {
            pks.add(Map.of("field", pk.field(), "types", pk.types()));
          }
          Map<String, Object> settings = new LinkedHashMap<>();
          settings.put("partition_keys", pks);
          settings.put("full_text_search_keys", o.settings().fullTextSearchKeys());
          settings.put("index_fields", o.settings().indexFields());
          settings.put("bloom_filter_fields", o.settings().bloomFilterFields());
          settings.put("data_retention", o.settings().retentionDays());
          return api.sendAsync(
                  TransportRequest.post(api.orgPath("/streams/" + name))
                      .query("type", o.type().wireValue())
                      .json(Map.of("fields", fields, "settings", settings))
                      .operation(op))
              .thenApply(r -> (Void) null);
        });
  }

  /**
   * Decodes a stream object of {@code /streams} or {@code /schema}.
   *
   * @param w JSON object
   * @return the stream
   */
  static StreamInfo info(JsonNode w) {
    JsonNode st = w.path("settings");
    List<String> distinct = new ArrayList<>();
    for (JsonNode d : st.path("distinct_value_fields")) {
      String n = d.isTextual() ? d.asText() : Values.text(d.get("name"));
      if (!n.isEmpty()) {
        distinct.add(n);
      }
    }
    StreamSettings settings =
        new StreamSettings(
            partitionKeys(st.path("partition_keys")),
            Values.strings(st.get("full_text_search_keys")),
            Values.strings(st.get("index_fields")),
            Values.strings(st.get("bloom_filter_fields")),
            distinct,
            st.path("data_retention").asInt(0),
            st.path("max_query_range").asInt(0));
    JsonNode stats = w.path("stats");
    long created = Values.asLong(stats.get("created_at"), 0);
    StreamStats s =
        new StreamStats(
            created > 0 ? Optional.of(Values.fromMicros(created)) : Optional.empty(),
            Values.asLong(stats.get("doc_num"), 0),
            Values.asLong(stats.get("file_num"), 0),
            stats.path("storage_size").asDouble(0),
            stats.path("compressed_size").asDouble(0));
    List<SchemaField> schema = new ArrayList<>();
    for (JsonNode f : w.path("schema")) {
      schema.add(new SchemaField(Values.text(f.get("name")), Values.text(f.get("type"))));
    }
    return new StreamInfo(
        Values.text(w.get("name")),
        StreamType.parse(Values.text(w.get("stream_type"))),
        Values.text(w.get("storage_type")),
        s,
        schema,
        settings,
        w.path("total_fields").asInt(schema.size()));
  }

  /** Decodes {@code {"L0": {...}, "L1": {...}}} in level order. */
  private static List<PartitionKey> partitionKeys(JsonNode node) {
    TreeMap<Integer, JsonNode> levels = new TreeMap<>();
    for (Map.Entry<String, JsonNode> e : node.properties()) {
      int level;
      try {
        level = Integer.parseInt(e.getKey().replaceFirst("^[Ll]", ""));
      } catch (NumberFormatException ex) {
        level = Integer.MAX_VALUE - levels.size();
      }
      levels.put(level, e.getValue());
    }
    List<PartitionKey> out = new ArrayList<>();
    for (JsonNode pk : levels.values()) {
      JsonNode types = pk.path("types");
      out.add(
          new PartitionKey(
              Values.text(pk.get("field")),
              types.isTextual() ? types.asText() : Values.text(types),
              pk.path("disabled").asBoolean(false)));
    }
    return out;
  }
}
