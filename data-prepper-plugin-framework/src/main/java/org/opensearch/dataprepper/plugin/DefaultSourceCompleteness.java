/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.SourceCompleteness;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default {@link SourceCompleteness} that aggregates per-partition completeness into three gauges
 * published through the source's {@link PluginMetrics}:
 * <ul>
 *   <li>{@value #COMPLETE_UP_TO}: epoch milliseconds of the oldest partition's complete-up-to time.
 *       Caught-up partitions count as "now". Reports 0 while no partition is reporting.</li>
 *   <li>{@value #FRESHNESS_LAG}: seconds between now and {@value #COMPLETE_UP_TO}. Reports 0 while
 *       no partition is reporting.</li>
 *   <li>{@value #REPORTING_PARTITIONS}: the number of partitions currently reporting.</li>
 * </ul>
 * Updates are monotonic per partition; an older value never replaces a newer one.
 */
public class DefaultSourceCompleteness implements SourceCompleteness {
    static final String COMPLETE_UP_TO = "completeUpTo";
    static final String FRESHNESS_LAG = "freshnessLag";
    static final String REPORTING_PARTITIONS = "completenessReportingPartitions";

    /** Marker meaning "this partition has no unread data and is complete up to now". */
    private static final Instant CAUGHT_UP = Instant.MAX;

    private final Map<String, Instant> partitions = new ConcurrentHashMap<>();
    private final Clock clock;

    public DefaultSourceCompleteness(final PluginMetrics pluginMetrics) {
        this(pluginMetrics, Clock.systemUTC());
    }

    DefaultSourceCompleteness(final PluginMetrics pluginMetrics, final Clock clock) {
        this.clock = Objects.requireNonNull(clock);
        Objects.requireNonNull(pluginMetrics);
        pluginMetrics.gauge(COMPLETE_UP_TO, this, c -> c.completeUpTo().map(Instant::toEpochMilli).orElse(0L));
        pluginMetrics.gauge(FRESHNESS_LAG, this, DefaultSourceCompleteness::freshnessLagSeconds);
        pluginMetrics.gauge(REPORTING_PARTITIONS, this, c -> c.partitions.size());
    }

    @Override
    public void update(final String partitionKey, final Instant completeUpTo) {
        Objects.requireNonNull(partitionKey);
        Objects.requireNonNull(completeUpTo);
        partitions.merge(partitionKey, completeUpTo, (current, incoming) -> {
            if (current == CAUGHT_UP) {
                // Leaving the caught-up state: the partition has unread data again.
                return incoming;
            }
            return incoming.isAfter(current) ? incoming : current;
        });
    }

    @Override
    public void markCaughtUp(final String partitionKey) {
        Objects.requireNonNull(partitionKey);
        partitions.put(partitionKey, CAUGHT_UP);
    }

    @Override
    public void release(final String partitionKey) {
        Objects.requireNonNull(partitionKey);
        partitions.remove(partitionKey);
    }

    /**
     * The oldest complete-up-to time across reporting partitions, or empty when nothing reports.
     * Caught-up partitions are evaluated as the current time.
     */
    Optional<Instant> completeUpTo() {
        if (partitions.isEmpty()) {
            return Optional.empty();
        }
        final Instant now = clock.instant();
        Instant oldest = null;
        for (final Instant value : partitions.values()) {
            final Instant effective = value == CAUGHT_UP ? now : value;
            if (oldest == null || effective.isBefore(oldest)) {
                oldest = effective;
            }
        }
        return Optional.ofNullable(oldest);
    }

    double freshnessLagSeconds() {
        return completeUpTo()
                .map(complete -> Math.max(0.0, (clock.millis() - complete.toEpochMilli()) / 1000.0))
                .orElse(0.0);
    }
}
