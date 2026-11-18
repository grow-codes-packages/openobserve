package codes.grow.packages.openobserve.logs;

import java.util.List;

/**
 * Outcome of {@link Logs#ingestJson} and {@link Logs#ingestMulti}. OpenObserve answers HTTP 200
 * even when some records were rejected: check {@link #failed()}.
 *
 * @param successful records stored (all streams)
 * @param failed records rejected (all streams)
 * @param streams per-stream outcome
 */
public record IngestResult(int successful, int failed, List<IngestStreamStatus> streams) {
  /**
   * Copies the stream list.
   *
   * @param successful records stored
   * @param failed records rejected
   * @param streams per-stream outcome
   */
  public IngestResult {
    streams = List.copyOf(streams);
  }
}
