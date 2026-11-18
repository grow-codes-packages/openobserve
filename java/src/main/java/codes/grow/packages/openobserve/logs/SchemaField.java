package codes.grow.packages.openobserve.logs;

/**
 * One field of a stream schema.
 *
 * @param name field name
 * @param type Arrow type name ({@code Utf8}, {@code Int64}, {@code Float64}, {@code Boolean}, …)
 */
public record SchemaField(String name, String type) {}
