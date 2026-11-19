package codes.grow.packages.openobserve.logs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.openobserve.Fixtures;
import codes.grow.packages.openobserve.OpenObserveClient;
import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.MockServer;
import codes.grow.packages.openobserve.core.NotFoundException;
import codes.grow.packages.openobserve.core.ValidationException;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StreamsTest {
  private MockServer server;
  private OpenObserveClient client;
  private Streams streams;

  @BeforeEach
  void start() throws Exception {
    server = MockServer.start();
    client = Fixtures.client(server).build();
    streams = client.streams();
  }

  @AfterEach
  void stop() {
    client.close();
    server.close();
  }

  private JsonNode body(int i) {
    return Json.tree(server.requests().get(i).body().getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void listStreams() {
    Fixtures.enqueue(server, "listStreams.logs");
    Fixtures.enqueue(server, "listStreams.paged_keyword");
    Fixtures.enqueue(server, "listStreams.traces");
    Fixtures.enqueue(server, "listStreams.logs");
    StreamList all = streams.listStreams();
    assertThat(all.streams()).isNotEmpty();
    assertThat(all.total()).isGreaterThanOrEqualTo(all.streams().size());
    StreamInfo first = all.streams().get(0);
    assertThat(first.type()).isEqualTo(StreamType.LOGS);
    assertThat(first.storageType()).isEqualTo("disk");
    assertThat(first.stats().createdAt()).isPresent();
    assertThat(server.requests().get(0).uri()).isEqualTo("/api/default/streams?type=logs");
    StreamList page =
        streams.listStreams(new ListStreamsOptions(StreamType.LOGS, "6moiazbq", 2, 1));
    assertThat(page.streams()).hasSize(1);
    assertThat(server.requests().get(1).uri())
        .contains("keyword=6moiazbq")
        .contains("offset=2")
        .contains("limit=1");
    StreamList traces = streams.listStreams(StreamType.TRACES);
    assertThat(traces.streams()).allSatisfy(s -> assertThat(s.type()).isEqualTo(StreamType.TRACES));
    streams.listStreamsAsync(null).join();
    assertThat(server.requests().get(3).uri()).isEqualTo("/api/default/streams?type=logs");
  }

  @Test
  void getStreamSchema() {
    Fixtures.enqueue(server, "getStreamSchema.after_settings");
    Fixtures.enqueue(server, "getStreamSchema.404");
    StreamInfo info = streams.getStreamSchema("Sdk-Logs");
    assertThat(info.schema()).contains(new SchemaField("message", "Utf8"));
    assertThat(info.settings().retentionDays()).isEqualTo(7);
    assertThat(info.settings().partitionKeys())
        .containsExactly(new PartitionKey("run_id", "value", true));
    assertThat(info.totalFields()).isEqualTo(8);
    assertThat(server.requests().get(0).uri())
        .isEqualTo("/api/default/streams/sdk_logs/schema?type=logs");
    assertThatThrownBy(() -> streams.getStreamSchema("missing", StreamType.TRACES))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("stream not found");
  }

  @Test
  void decodesEveryInfoShape() {
    JsonNode w =
        Json.tree(
            ("{\"name\":\"s\",\"stream_type\":\"metrics\",\"stats\":{\"created_at\":0,"
                    + "\"doc_num\":\"5\",\"file_num\":2,\"storage_size\":1.5,\"compressed_size\":0.5},"
                    + "\"settings\":{\"partition_keys\":{\"L1\":{\"field\":\"b\",\"types\":{\"hash\":8}},"
                    + "\"L0\":{\"field\":\"a\"},\"x\":{\"field\":\"c\",\"types\":\"value\"}},"
                    + "\"distinct_value_fields\":[\"d1\",{\"name\":\"d2\"},{\"other\":1}],"
                    + "\"full_text_search_keys\":[\"body\"],\"max_query_range\":4},"
                    + "\"schema\":[{\"name\":\"f\",\"type\":\"Utf8\"}]}")
                .getBytes(StandardCharsets.UTF_8));
    StreamInfo i = Streams.info(w);
    assertThat(i.type()).isEqualTo(StreamType.METRICS);
    assertThat(i.stats().createdAt()).isEmpty();
    assertThat(i.stats().docCount()).isEqualTo(5);
    assertThat(i.stats().fileCount()).isEqualTo(2);
    assertThat(i.stats().storageSize()).isEqualTo(1.5);
    assertThat(i.stats().compressedSize()).isEqualTo(0.5);
    assertThat(i.settings().partitionKeys())
        .extracting(PartitionKey::field)
        .containsExactly("a", "b", "c");
    assertThat(i.settings().partitionKeys().get(0).types()).isEqualTo("value");
    assertThat(i.settings().partitionKeys().get(1).types()).isEqualTo("{\"hash\":8}");
    assertThat(i.settings().distinctValueFields()).containsExactly("d1", "d2");
    assertThat(i.settings().fullTextSearchKeys()).containsExactly("body");
    assertThat(i.settings().maxQueryRange()).isEqualTo(4);
    assertThat(i.totalFields()).isEqualTo(1);
  }

  @Test
  void updateStreamSettingsSendsDeltas() {
    server.enqueue(
        200,
        "{\"name\":\"s\",\"settings\":{\"partition_keys\":{\"L0\":{\"field\":\"old\",\"types\":\"value\"},"
            + "\"L1\":{\"field\":\"gone\",\"types\":\"value\",\"disabled\":true},"
            + "\"L2\":{\"field\":\"keep\",\"types\":\"value\"}},"
            + "\"full_text_search_keys\":[\"log\",\"msg\"],\"index_fields\":[\"level\"]}}");
    Fixtures.enqueue(server, "updateStreamSettings.ok");
    streams.updateStreamSettings(
        "s",
        StreamSettingsUpdate.builder()
            .retentionDays(7)
            .fullTextSearchKeys(List.of("message", "msg", "message"))
            .partitionKeys(List.of(PartitionKey.of("run_id"), PartitionKey.of("keep")))
            .indexFields(List.of("level"))
            .bloomFilterFields(List.of("trace_id"))
            .build());
    assertThat(server.requests().get(0).uri()).isEqualTo("/api/default/streams/s/schema?type=logs");
    MockServer.Recorded put = server.requests().get(1);
    assertThat(put.method()).isEqualTo("PUT");
    assertThat(put.uri()).isEqualTo("/api/default/streams/s/settings?type=logs");
    JsonNode b = body(1);
    assertThat(b.get("data_retention").asInt()).isEqualTo(7);
    assertThat(b.get("full_text_search_keys").toString())
        .isEqualTo("{\"add\":[\"message\"],\"remove\":[\"log\"]}");
    assertThat(b.get("partition_keys").get("add").get(0).get("field").asText()).isEqualTo("run_id");
    assertThat(b.get("partition_keys").get("remove").get(0).get("field").asText()).isEqualTo("old");
    assertThat(b.get("partition_keys").get("remove")).hasSize(1);
    assertThat(b.has("index_fields")).isFalse();
    assertThat(b.get("bloom_filter_fields").toString())
        .isEqualTo("{\"add\":[\"trace_id\"],\"remove\":[]}");
  }

  @Test
  void updateStreamSettingsEdgeCases() {
    // retention only: no schema read
    Fixtures.enqueue(server, "updateStreamSettings.ok");
    streams.updateStreamSettings(
        "s", StreamSettingsUpdate.builder().type(StreamType.TRACES).retentionDays(1).build());
    assertThat(server.requests().get(0).uri())
        .isEqualTo("/api/default/streams/s/settings?type=traces");
    // unchanged lists: nothing is sent after the schema read
    server.enqueue(
        200,
        "{\"name\":\"s\",\"settings\":{\"partition_keys\":{\"L0\":{\"field\":\"a\",\"types\":\"value\"}},"
            + "\"full_text_search_keys\":[\"m\"]}}");
    streams
        .updateStreamSettingsAsync(
            "s",
            StreamSettingsUpdate.builder()
                .fullTextSearchKeys(List.of("m"))
                .partitionKeys(List.of(PartitionKey.of("a")))
                .build())
        .join();
    assertThat(server.requests()).hasSize(2);
    streams.updateStreamSettings("s", StreamSettingsUpdate.builder().build());
    assertThat(server.requests()).hasSize(2);
    Fixtures.enqueue(server, "updateStreamSettings.404");
    assertThatThrownBy(
            () ->
                streams.updateStreamSettings(
                    "s", StreamSettingsUpdate.builder().retentionDays(2).build()))
        .isInstanceOf(NotFoundException.class);
    assertThatThrownBy(
            () ->
                streams.updateStreamSettings(
                    "s", StreamSettingsUpdate.builder().retentionDays(-1).build()))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> streams.updateStreamSettings("s", null))
        .isInstanceOf(ValidationException.class);
    StreamSettingsUpdate u = StreamSettingsUpdate.builder().type(null).build();
    assertThat(u.type()).isEqualTo(StreamType.LOGS);
    assertThat(u.retentionDays()).isEmpty();
    assertThat(u.partitionKeys()).isEmpty();
    assertThat(u.fullTextSearchKeys()).isEmpty();
    assertThat(Streams.stringDelta(List.of("a"), List.of("a"))).isNull();
    assertThat(Streams.partitionDelta(List.of(), List.of())).isNull();
  }

  @Test
  void deleteAndCreate() {
    Fixtures.enqueue(server, "deleteStream.ok");
    Fixtures.enqueue(server, "deleteStream.404");
    Fixtures.enqueue(server, "createStream.ok");
    Fixtures.enqueue(server, "createStream.ok");
    streams.deleteStream("s");
    assertThat(server.requests().get(0).method()).isEqualTo("DELETE");
    assertThat(server.requests().get(0).uri()).isEqualTo("/api/default/streams/s?type=logs");
    assertThatThrownBy(() -> streams.deleteStreamAsync("s", StreamType.LOGS).join())
        .hasCauseInstanceOf(NotFoundException.class);
    streams.createStream(
        "Created",
        new CreateStreamOptions(
            StreamType.LOGS,
            List.of(new SchemaField("message", "Utf8")),
            new StreamSettings(
                List.of(new PartitionKey("run_id", null, false)),
                List.of("message"),
                List.of("i"),
                List.of("b"),
                null,
                3,
                0)));
    assertThat(server.requests().get(2).uri()).isEqualTo("/api/default/streams/created?type=logs");
    JsonNode b = body(2);
    assertThat(b.get("fields").get(0).get("name").asText()).isEqualTo("message");
    JsonNode st = b.get("settings");
    assertThat(st.get("data_retention").asInt()).isEqualTo(3);
    assertThat(st.get("partition_keys").get(0).get("types").asText()).isEqualTo("value");
    assertThat(st.get("full_text_search_keys").get(0).asText()).isEqualTo("message");
    assertThat(st.get("index_fields").get(0).asText()).isEqualTo("i");
    assertThat(st.get("bloom_filter_fields").get(0).asText()).isEqualTo("b");
    streams.createStreamAsync("plain", null).join();
    assertThat(body(3).get("fields")).isEmpty();
    assertThatThrownBy(() -> streams.createStream("", CreateStreamOptions.defaults()))
        .isInstanceOf(ValidationException.class);
    assertThat(Map.of("k", StreamSettings.empty().partitionKeys())).containsKey("k");
  }
}
