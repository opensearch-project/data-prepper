/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Interface to be passed to {@link org.opensearch.dataprepper.model.source.Source} to enable distribution and coordination of work
 * between source plugins, and to track progress and save state for these partitions of work
 * @since 2.2
 */
public interface SourceCoordinator {

    /**
     * A partition will be created for each of the partitionKeys passed.
     * @param partitionKeys
     */
    void createPartitions(final List<String> partitionKeys);

    /**
     * This should be called by the source when it needs to get the next partition it should process on
     * @return {@link org.opensearch.dataprepper.model.source.SourcePartition} with the details about how to process this partition
     */
    Optional<SourcePartition> getNextPartition();

    /**
     * Should be called by the source when it has fully processed a given partition
     * @param partitionKey - The partitionKey that identifies the partition of work that was fully processed
     */
    void completePartition(final String partitionKey);

    /**
     * Should be called by the source when it has processed all that it can up to this point in time for a given partition,
     * but the partition is expected to need work done on it in the future.
     * @param partitionKey - The partitionKey that identifies the partition of work that should be
     * @param reopenAfter - The duration from the current time to wait before this partition should be processed further at a later date
     */
    void closePartition(final String partitionKey, final Duration reopenAfter);

    /**
     * Should be called by the source when it has completed some work for a partition, and needs to save its progress before continuing work on the partition.
     * @param partitionKey
     * @param partitionState
     */
    void saveStateForPartition(final String partitionKey, final Object partitionState);

    /**
     * Should be called by the source when it is shutting down to indicate that it will no longer be able to perform work on partitions
     */
    void giveUpPartitions();
}
