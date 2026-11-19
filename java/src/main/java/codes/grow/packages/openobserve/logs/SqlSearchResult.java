package codes.grow.packages.openobserve.logs;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * One page of an OpenObserve SQL search.
 *
 * @param hits result rows (flat maps; numbers are {@code Long} or {@code Double})
 * @param total number of matching rows, set only with {@code trackTotalHits} (the server's own
 *     {@code total} is the page length)
 * @param from row offset
 * @param size page size
 * @param took server time in milliseconds
 * @param scanRecords records the server scanned
 * @param searchId the server's search trace id (not telemetry)
 * @param partial whether the result is partial
 * @param histogramIntervalSeconds bucket width in seconds for histogram queries (0 otherwise)
 */
public record SqlSearchResult(
    List<Map<String, Object>> hits,
    OptionalLong total,
    int from,
    int size,
    long took,
    long scanRecords,
    Optional<String> searchId,
    boolean partial,
    long histogramIntervalSeconds) {
  /**
   * Copies the rows and normalizes null optionals.
   *
   * @param hits rows
   * @param total total
   * @param from offset
   * @param size page size
   * @param took server time
   * @param scanRecords scanned records
   * @param searchId search id
   * @param partial partial flag
   * @param histogramIntervalSeconds histogram interval
   */
  public SqlSearchResult {
    hits = List.copyOf(hits);
    total = total == null ? OptionalLong.empty() : total;
    searchId = searchId == null ? Optional.empty() : searchId;
  }

  /**
   * Copy with a total.
   *
   * @param count total
   * @return a new result
   */
  SqlSearchResult withTotal(long count) {
    return new SqlSearchResult(
        hits,
        OptionalLong.of(count),
        from,
        size,
        took,
        scanRecords,
        searchId,
        partial,
        histogramIntervalSeconds);
  }
}
