package codes.grow.packages.openobserve.logs;

import java.util.Optional;

/**
 * Outcome of one bulk action.
 *
 * @param action action key reported by the server ({@code index} also for {@code create})
 * @param index stream
 * @param status per-item HTTP status (200 stored, 4xx rejected)
 * @param error {@code "<type>: <reason>"} for a rejected item
 */
public record BulkItem(String action, String index, int status, Optional<String> error) {
  /**
   * Normalizes a null error.
   *
   * @param action action
   * @param index stream
   * @param status status
   * @param error error
   */
  public BulkItem {
    error = error == null ? Optional.empty() : error;
  }
}
