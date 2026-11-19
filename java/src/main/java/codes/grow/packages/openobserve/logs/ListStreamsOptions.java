package codes.grow.packages.openobserve.logs;

/**
 * Options of {@link Streams#listStreams}.
 *
 * @param type stream type (null = logs; always sent, since the server otherwise mixes every type)
 * @param keyword keep streams whose name contains it (null = all)
 * @param offset page offset (0 = start)
 * @param limit page size (0 = server default)
 */
public record ListStreamsOptions(StreamType type, String keyword, int offset, int limit) {
  /**
   * Defaults a null type to logs.
   *
   * @param type type
   * @param keyword keyword
   * @param offset offset
   * @param limit limit
   */
  public ListStreamsOptions {
    type = StreamType.orLogs(type);
  }

  /**
   * Every stream of a type.
   *
   * @param type type
   * @return options
   */
  public static ListStreamsOptions of(StreamType type) {
    return new ListStreamsOptions(type, null, 0, 0);
  }
}
