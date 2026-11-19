package codes.grow.packages.openobserve.logs;

/**
 * Outcome of {@link Logs#ingestOtlp}.
 *
 * @param accepted records stored
 * @param rejected records the server reported in {@code partialSuccess}
 */
public record OtlpIngestResult(int accepted, int rejected) {}
