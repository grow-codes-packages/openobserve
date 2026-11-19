package codes.grow.packages.openobserve.logs;

import java.util.List;

/**
 * A stream with its schema, settings and statistics.
 *
 * @param name stream name
 * @param type stream type
 * @param storageType storage type ({@code disk}, {@code s3}, …)
 * @param stats statistics
 * @param schema schema fields
 * @param settings settings
 * @param totalFields number of fields
 */
public record StreamInfo(
    String name,
    StreamType type,
    String storageType,
    StreamStats stats,
    List<SchemaField> schema,
    StreamSettings settings,
    int totalFields) {
  /**
   * Copies the schema.
   *
   * @param name name
   * @param type type
   * @param storageType storage type
   * @param stats stats
   * @param schema schema
   * @param settings settings
   * @param totalFields field count
   */
  public StreamInfo {
    schema = List.copyOf(schema);
  }
}
