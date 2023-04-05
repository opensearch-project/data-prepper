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
     * @param partitionIdentifiers partition identifiers. Each should have a unique partition key
     * @since 2.2
     */
    void createPartitions(final List<PartitionIdentifier> partitionIdentifiers);

    /**
     * This should be called by the source when it needs to get the next partition it should process on. Also is a way to renew ownership of a partition that is actively being worked on
     *
     * @return {@link SourcePartition} with the details about how to process this partition. Will repeatedly return the partition until
     * {@link SourceCoordinator#completePartition(String)}
     * or {@link SourceCoordinator#closePartition(String, Duration)} are called by the source,
     * or until the partition ownership times out.
     * @since 2.2
     */
    Optional<SourcePartition<T>> getNextPartition();

    /**
     * Should be called by the source when it has fully processed a given partition
     *
     * @param partitionKey - The partition key that uniquely identifies the partition of work that was fully processed
     * @return Whether the partition was marked as completed successfully or not. If this returns false, the operation is safe to retry.
     * @since 2.2
     */
    boolean completePartition(final String partitionKey);

    /**
     * Should be called by the source when it has processed all that it can up to this point in time for a given partition,
     * but the partition is expected to need work done on it in the future. While not required, it is necessary in most cases to call saveStateForPartition before closing it.
     * Otherwise, when the partition reopens, the state will be outdated and duplicate parts of the partition will be processed.
     *
     * @param partitionKey - The partition key that uniquely identifies the partition of work
     * @param reopenAfter  - The duration from the current time to wait before this partition should be processed further at a later date
     * @return Whether the partition was closed successfully or not. If this returns false, the operation is safe to retry.
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException if the partition key could not be found in the distributed store
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException if the partition is not owned by this instance of SourceCoordinator
     * @since 2.2
     */
    boolean closePartition(final String partitionKey, final Duration reopenAfter);

    /**
     * Should be called by the source when it has completed some work for a partition, and needs to save its progress before continuing work on the partition.
     * Should also be called before closePartition and giveUpPartitions if state needs to be updated.
     * Can optionally be called before completePartition if the progress state should be audited.
     *
     * @param <S>                    The partition state type
     * @param partitionKey           partition key that uniquely represents the partition
     * @param partitionProgressState The object to represent the latest partition progress state
     * @return Whether the state was saved successfully for the partition. If the return value is false, the source can choose to retry
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException if the partition key could not be found in the distributed store.
             Grab a new partition to process with {@link #getNextPartition()}
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException if the partition is not owned by this instance of SourceCoordinator.
     *      Grab a new partition to process with {@link #getNextPartition()}
     * @since 2.2
     */
    <S extends T> boolean saveProgressStateForPartition(final String partitionKey, final S partitionProgressState);

    /**
     * Should be called by the source when it is shutting down to indicate that it will no longer be able to perform work on partitions,
     * or can be called to give up ownership of its partitions in order to pick up new ones with {@link #getNextPartition()}.
     * @since 2.2
     */
    void giveUpPartitions();
}
