package codes.grow.packages.openobserve.logs;

/**
 * One distinct value with its number of records.
 *
 * @param value the value ({@code String}, {@code Long}, {@code Double} or {@code Boolean})
 * @param count number of records
 */
public record ValueCount(Object value, long count) {}
