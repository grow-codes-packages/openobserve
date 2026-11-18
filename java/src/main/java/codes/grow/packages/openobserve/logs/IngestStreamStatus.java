package codes.grow.packages.openobserve.logs;

import java.util.Optional;

/**
 * Per-stream outcome of an ingest call.
 *
 * @param name stream name as normalized by the server
 * @param successful records stored
 * @param failed records rejected (for example older than the ingest window)
 * @param error last rejection reason the server kept for this stream
 */
public record IngestStreamStatus(String name, int successful, int failed, Optional<String> error) {
  /**
   * Normalizes a null error.
   *
   * @param name stream name
   * @param successful records stored
   * @param failed records rejected
   * @param error rejection reason
   */
  public IngestStreamStatus {
    error = error == null ? Optional.empty() : error;
  }
}
