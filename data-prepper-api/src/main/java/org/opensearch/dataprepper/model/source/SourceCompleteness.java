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

package org.opensearch.dataprepper.model.source;

import java.time.Instant;

/**
 * Lets a {@link Source} report how far it has fully delivered each partition it reads.
 * <p>
 * A source calls {@link #update(String, Instant)} when it knows that every record from a partition
 * whose ordering time is at or before the given instant has been handed to the pipeline (written
 * to the buffer, or acknowledged when end-to-end acknowledgements are enabled). The framework
 * aggregates these per-partition values into the {@code completeUpTo} and {@code freshnessLag}
 * gauges for the source.
 * <p>
 * Sources that never call this interface are simply not included in completeness metrics. All
 * methods are cheap and thread-safe. Sources receive an instance through constructor injection,
 * in the same way as {@code PluginMetrics}.
 *
 * @since 2.17
 */
public interface SourceCompleteness {

    /**
     * Reports that the partition has delivered everything up to and including {@code completeUpTo}.
     * Values older than the current value for the partition are ignored, so a partition never moves
     * backwards.
     *
     * @param partitionKey a stable identifier for the partition within this source, for example
     *                     {@code "orders-0"} for a Kafka topic partition
     * @param completeUpTo the ordering time up to which the partition is complete
     */
    void update(String partitionKey, Instant completeUpTo);

    /**
     * Reports that the partition currently has no unread data. A caught-up partition is treated as
     * complete up to the current time on every read, so an idle partition never makes the source
     * look stale.
     *
     * @param partitionKey the partition identifier
     */
    void markCaughtUp(String partitionKey);

    /**
     * Reports that this instance no longer reads the partition, for example because it was revoked
     * in a consumer-group rebalance or the partition is finished. The partition leaves the
     * aggregate until a subsequent {@link #update(String, Instant)} or
     * {@link #markCaughtUp(String)}.
     *
     * @param partitionKey the partition identifier
     */
    void release(String partitionKey);
}
