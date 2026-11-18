package codes.grow.packages.openobserve.logs;

import java.util.List;

/**
 * Outcome of {@link Logs#ingestBulk}. {@link #errors()} is true when at least one item was rejected
 * (the HTTP status is still 200).
 *
 * @param took server time in milliseconds
 * @param errors whether any item was rejected
 * @param items per-action outcome
 */
public record BulkResult(long took, boolean errors, List<BulkItem> items) {
  /**
   * Copies the item list.
   *
   * @param took server time
   * @param errors whether any item failed
   * @param items items
   */
  public BulkResult {
    items = List.copyOf(items);
  }
}
