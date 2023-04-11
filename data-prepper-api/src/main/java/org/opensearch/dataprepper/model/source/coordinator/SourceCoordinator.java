/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Interface to be passed to {@link org.opensearch.dataprepper.model.source.Source} to enable distribution and coordination of work
 * between source plugins, and to track progress and save state for these partitions of work
 * @param <T> The model type to represent the partition progress state
 * @since 2.2
 */
public interface SourceCoordinator<T> {

    /**
     * A partition will be created for each of the partitionKeys passed. Can be called either on a schedule every once in a while to pick up new partitions,
     * or only when needed if the source is capable of being notified when new partitions are created.
     * @param partitionIdentifiers partition identifiers
     * @since 2.2
     */
    void createPartitions(final List<PartitionIdentifier> partitionIdentifiers);

    /**
     * This should be called by the source when it needs to get the next partition it should process on. Also is a way to renew ownership of a partition that is actively being worked on
     *
     * @return {@link SourcePartition} with the details about how to process this partition. Will repeatedly return the partition until
     * {@link SourceCoordinator#completePartition(PartitionIdentifier)}
     * or {@link SourceCoordinator#closePartition(PartitionIdentifier, Duration)} are called by the source,
     * or until the partition ownership times out.
     * @since 2.2
     */
    Optional<SourcePartition<T>> getNextPartition();

    /**
     * Should be called by the source when it has fully processed a given partition
     * @param partitionIdentifier - The partition that identifies the partition of work that was fully processed
     * @since 2.2
     */
    void completePartition(final PartitionIdentifier partitionIdentifier);

    /**
     * Should be called by the source when it has processed all that it can up to this point in time for a given partition,
     * but the partition is expected to need work done on it in the future. While not required, it is necessary in most cases to call saveStateForPartition before closing it.
     * Otherwise, when the partition reopens, the state will be outdated and duplicate parts of the partition will be processed.
     * @param partitionIdentifier - The partition that identifies the partition of work
     * @param reopenAfter - The duration from the current time to wait before this partition should be processed further at a later date
     * @since 2.2
     */
    void closePartition(final PartitionIdentifier partitionIdentifier, final Duration reopenAfter);

    /**
     * Should be called by the source when it has completed some work for a partition, and needs to save its progress before continuing work on the partition.
     * Should also be called before closePartition and giveUpPartitions if state needs to be updated.
     * Can optionally be called before completePartition if the progress state should be audited.
     * @param partitionIdentifier partition identifier
     * @param partitionProgressState The object to represent the latest partition progress state
     * @param <S> The partition state type
     * @since 2.2
     */
    <S extends T> void saveProgressStateForPartition(final PartitionIdentifier partitionIdentifier, final S partitionProgressState);

    /**
     * Should be called by the source when it is shutting down to indicate that it will no longer be able to perform work on partitions,
     * or can be called to give up ownership of its partitions in order to pick up new ones.
     * @since 2.2
     */
    void giveUpPartitions();
}
