package codes.grow.packages.openobserve.logs;

import java.util.List;

/**
 * Settings of a stream.
 *
 * @param partitionKeys partition keys in level order (removed keys stay, disabled)
 * @param fullTextSearchKeys full-text search keys
 * @param indexFields secondary index fields
 * @param bloomFilterFields bloom filter fields
 * @param distinctValueFields distinct value fields
 * @param retentionDays data retention in days (0 = server default)
 * @param maxQueryRange maximum query range in hours (0 = unlimited)
 */
public record StreamSettings(
    List<PartitionKey> partitionKeys,
    List<String> fullTextSearchKeys,
    List<String> indexFields,
    List<String> bloomFilterFields,
    List<String> distinctValueFields,
    int retentionDays,
    int maxQueryRange) {
  /**
   * Copies the lists (null means empty).
   *
   * @param partitionKeys partition keys
   * @param fullTextSearchKeys FTS keys
   * @param indexFields index fields
   * @param bloomFilterFields bloom filter fields
   * @param distinctValueFields distinct value fields
   * @param retentionDays retention
   * @param maxQueryRange max query range
   */
  public StreamSettings {
    partitionKeys = partitionKeys == null ? List.of() : List.copyOf(partitionKeys);
    fullTextSearchKeys = fullTextSearchKeys == null ? List.of() : List.copyOf(fullTextSearchKeys);
    indexFields = indexFields == null ? List.of() : List.copyOf(indexFields);
    bloomFilterFields = bloomFilterFields == null ? List.of() : List.copyOf(bloomFilterFields);
    distinctValueFields =
        distinctValueFields == null ? List.of() : List.copyOf(distinctValueFields);
  }

  /**
   * Empty settings (server defaults).
   *
   * @return settings
   */
  public static StreamSettings empty() {
    return new StreamSettings(null, null, null, null, null, 0, 0);
  }
}
