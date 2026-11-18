package codes.grow.packages.openobserve.logs;

import java.util.List;

/**
 * The most frequent values of one field.
 *
 * @param field field name
 * @param values values, most frequent first
 */
public record FieldValues(String field, List<ValueCount> values) {
  /**
   * Copies the values.
   *
   * @param field field
   * @param values values
   */
  public FieldValues {
    values = List.copyOf(values);
  }
}
