/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Plugin-side view of shard statistics returned by a search request. Keeps the
 * plugin decoupled from the underlying client (OpenSearch or Elasticsearch) types.
 *
 * Failure reasons are aggregated into a normalized map of reason to occurrence
 * count so instances remain small even when a cluster returns many shard
 * failures. The map is capped at {@link #MAX_DISTINCT_REASONS} distinct keys;
 * further unique reasons increment the {@link #OVERFLOW_REASON_KEY} bucket
 * instead of growing the map.
 */
public class SearchShardStatistics {

    public static final int MAX_DISTINCT_REASONS = 20;
    public static final String OVERFLOW_REASON_KEY = "__other__";

    private static final Pattern UUID_PATTERN = Pattern.compile(
            "\\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\\b");
    private static final Pattern SHARD_ID_PATTERN = Pattern.compile("\\[[^\\]]*\\]\\[\\d+\\]");
    private static final Pattern NODE_ID_PATTERN = Pattern.compile("node\\[[^\\]]+\\]");
    private static final int MAX_REASON_KEY_LENGTH = 512;
    private static final String UNKNOWN_REASON = "unknown";

    private static final SearchShardStatistics EMPTY = new SearchShardStatistics(0, 0, 0, 0, Collections.emptyMap());

    private final int total;
    private final int successful;
    private final int failed;
    private final int skipped;
    private final Map<String, Long> failureReasonCounts;

    public SearchShardStatistics(final int total,
                                 final int successful,
                                 final int failed,
                                 final int skipped,
                                 final Map<String, Long> failureReasonCounts) {
        this.total = total;
        this.successful = successful;
        this.failed = failed;
        this.skipped = skipped;
        this.failureReasonCounts = failureReasonCounts == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<>(failureReasonCounts));
    }

    public static SearchShardStatistics empty() {
        return EMPTY;
    }

    public int getTotal() {
        return total;
    }

    public int getSuccessful() {
        return successful;
    }

    public int getFailed() {
        return failed;
    }

    public int getSkipped() {
        return skipped;
    }

    public Map<String, Long> getFailureReasonCounts() {
        return failureReasonCounts;
    }

    public boolean hasFailures() {
        return failed > 0 || !failureReasonCounts.isEmpty();
    }

    /**
     * Normalize a raw failure reason into a stable key by stripping shard ids,
     * node ids, and UUIDs so unrelated occurrences of the same underlying error
     * collapse to a single map key.
     */
    public static String normalizeReason(final String rawReason) {
        if (rawReason == null || rawReason.isEmpty()) {
            return UNKNOWN_REASON;
        }
        String normalized = rawReason;
        normalized = SHARD_ID_PATTERN.matcher(normalized).replaceAll("[shard]");
        normalized = NODE_ID_PATTERN.matcher(normalized).replaceAll("node[?]");
        normalized = UUID_PATTERN.matcher(normalized).replaceAll("<uuid>");
        normalized = normalized.trim();
        if (normalized.isEmpty()) {
            return UNKNOWN_REASON;
        }
        if (normalized.length() > MAX_REASON_KEY_LENGTH) {
            normalized = normalized.substring(0, MAX_REASON_KEY_LENGTH);
        }
        return normalized;
    }

    /**
     * Normalize a shard-failure cause expressed as a type + message pair.
     * Typically {@code type} is the error class from the client (for example
     * {@code shard_failure} or {@code illegal_argument_exception}) and
     * {@code message} is the human-readable reason string.
     */
    public static String normalizeReason(final String type, final String message) {
        final boolean hasType = type != null && !type.isEmpty();
        final boolean hasMessage = message != null && !message.isEmpty();
        if (!hasType && !hasMessage) {
            return UNKNOWN_REASON;
        }
        if (hasType && hasMessage) {
            return normalizeReason(type + ": " + message);
        }
        return normalizeReason(hasType ? type : message);
    }

    /**
     * Increment the count for a normalized reason in the given map, respecting
     * the {@link #MAX_DISTINCT_REASONS} cap. Existing keys always increment.
     * New keys are added until the cap is reached; after that they fold into
     * {@link #OVERFLOW_REASON_KEY}.
     */
    public static void incrementFailureReasonCount(final Map<String, Long> counts,
                                                   final String normalizedReason,
                                                   final long delta) {
        Objects.requireNonNull(counts, "counts");
        if (normalizedReason == null || delta <= 0) {
            return;
        }
        if (counts.containsKey(normalizedReason)) {
            counts.merge(normalizedReason, delta, Long::sum);
            return;
        }
        if (counts.size() < MAX_DISTINCT_REASONS) {
            counts.put(normalizedReason, delta);
            return;
        }
        counts.merge(OVERFLOW_REASON_KEY, delta, Long::sum);
    }

    /**
     * Merge another map of reason to count into the given counts map, respecting
     * the cap. Useful when aggregating per-response statistics into a running
     * total (for example in persisted progress state).
     */
    public static void mergeFailureReasonCounts(final Map<String, Long> counts,
                                                final Map<String, Long> toMerge) {
        if (toMerge == null || toMerge.isEmpty()) {
            return;
        }
        for (final Map.Entry<String, Long> entry : toMerge.entrySet()) {
            incrementFailureReasonCount(counts, entry.getKey(), entry.getValue() == null ? 0L : entry.getValue());
        }
    }

    /**
     * Utility: convert a nullable {@link Number} to int, defaulting to 0.
     * Useful when extracting shard counts from client response types that
     * may return boxed numbers or null.
     */
    public static int numberOrZero(final Number number) {
        return number == null ? 0 : number.intValue();
    }

    /**
     * Build a {@link SearchShardStatistics} from raw shard counts and a list
     * of (type, message) pairs representing individual shard failures. Each
     * pair is normalized and aggregated into the capped failure-reason map.
     *
     * @param total      total shard count (nullable)
     * @param successful successful shard count (nullable)
     * @param failed     failed shard count (nullable)
     * @param skipped    skipped shard count (nullable)
     * @param failures   list of [type, message] pairs; may be null or empty
     */
    public static SearchShardStatistics fromShardCounts(final Number total,
                                                        final Number successful,
                                                        final Number failed,
                                                        final Number skipped,
                                                        final List<String[]> failures) {
        final Map<String, Long> failureReasonCounts = new LinkedHashMap<>();
        if (failures != null) {
            for (final String[] pair : failures) {
                final String type = pair != null && pair.length > 0 ? pair[0] : null;
                final String message = pair != null && pair.length > 1 ? pair[1] : null;
                final String key = normalizeReason(type, message);
                incrementFailureReasonCount(failureReasonCounts, key, 1L);
            }
        }
        return new SearchShardStatistics(
                numberOrZero(total),
                numberOrZero(successful),
                numberOrZero(failed),
                numberOrZero(skipped),
                failureReasonCounts);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof SearchShardStatistics)) return false;
        final SearchShardStatistics that = (SearchShardStatistics) o;
        return total == that.total && successful == that.successful && failed == that.failed
                && skipped == that.skipped && Objects.equals(failureReasonCounts, that.failureReasonCounts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, successful, failed, skipped, failureReasonCounts);
    }
}
