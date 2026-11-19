package codes.grow.packages.openobserve.logs;

import java.util.List;

/**
 * Options of {@link Streams#createStream}.
 *
 * @param type stream type (null = logs)
 * @param fields schema fields to pre-declare (null = none)
 * @param settings initial settings (partition keys, full-text search keys, index fields, bloom
 *     filter fields and retention are sent; null = server defaults)
 */
public record CreateStreamOptions(
    StreamType type, List<SchemaField> fields, StreamSettings settings) {
  /**
   * Normalizes null values.
   *
   * @param type type
   * @param fields fields
   * @param settings settings
   */
  public CreateStreamOptions {
    type = StreamType.orLogs(type);
    fields = fields == null ? List.of() : List.copyOf(fields);
    settings = settings == null ? StreamSettings.empty() : settings;
  }

  /**
   * Defaults: a logs stream without pre-declared fields.
   *
   * @return options
   */
  public static CreateStreamOptions defaults() {
    return new CreateStreamOptions(null, null, null);
  }
}
