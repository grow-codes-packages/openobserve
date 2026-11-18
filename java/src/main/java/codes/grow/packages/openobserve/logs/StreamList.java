package codes.grow.packages.openobserve.logs;

import java.util.List;

/**
 * One page of streams.
 *
 * @param streams streams of the page
 * @param total number of every matching stream
 */
public record StreamList(List<StreamInfo> streams, int total) {
  /**
   * Copies the streams.
   *
   * @param streams streams
   * @param total total
   */
  public StreamList {
    streams = List.copyOf(streams);
  }
}
