package codes.grow.packages.openobserve.logs;

/**
 * A partition key of a stream.
 *
 * @param field field name
 * @param types partition type, {@code value} (default) or a hash spec as JSON text
 * @param disabled true for a key that was removed (OpenObserve keeps it, disabled)
 */
public record PartitionKey(String field, String types, boolean disabled) {
  /**
   * Defaults a null or empty type to {@code value}.
   *
   * @param field field
   * @param types type
   * @param disabled disabled flag
   */
  public PartitionKey {
    types = types == null || types.isEmpty() ? "value" : types;
  }

  /**
   * An enabled {@code value} partition key.
   *
   * @param field field name
   * @return the key
   */
  public static PartitionKey of(String field) {
    return new PartitionKey(field, "value", false);
  }
}
