package codes.grow.packages.openobserve.logs;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One Elasticsearch-style bulk action for {@link Logs#ingestBulk}. Only {@code index} and {@code
 * create} are accepted: OpenObserve silently ignores {@code delete} and {@code update}, so the SDK
 * rejects them with a {@code ValidationException}.
 *
 * @param action {@link #INDEX} (default) or {@link #CREATE}
 * @param index target stream
 * @param document the record
 */
public record BulkAction(String action, String index, Map<String, Object> document) {
  /** The {@code index} action. */
  public static final String INDEX = "index";

  /** The {@code create} action. */
  public static final String CREATE = "create";

  /**
   * Copies the document; a null action means {@link #INDEX}.
   *
   * @param action action
   * @param index target stream
   * @param document record
   */
  public BulkAction {
    action = action == null || action.isEmpty() ? INDEX : action;
    document =
        document == null
            ? null
            : java.util.Collections.unmodifiableMap(new LinkedHashMap<>(document));
  }

  /**
   * An {@code index} action.
   *
   * @param stream target stream
   * @param document record
   * @return the action
   */
  public static BulkAction index(String stream, Map<String, ?> document) {
    return new BulkAction(INDEX, stream, copy(document));
  }

  /**
   * A {@code create} action.
   *
   * @param stream target stream
   * @param document record
   * @return the action
   */
  public static BulkAction create(String stream, Map<String, ?> document) {
    return new BulkAction(CREATE, stream, copy(document));
  }

  private static Map<String, Object> copy(Map<String, ?> document) {
    Objects.requireNonNull(document, "document");
    return new LinkedHashMap<>(document);
  }
}
