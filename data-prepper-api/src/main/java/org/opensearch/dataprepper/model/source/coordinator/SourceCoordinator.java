/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Interface to be passed to {@link org.opensearch.dataprepper.model.source.Source} to enable distribution and coordination of work
 * between source plugins, and to track progress and save state for these partitions of work
 * @param <T> The model type to represent the partition progress state
 * @since 2.2
 */
public interface SourceCoordinator<T> {

    /**
     * This method should be called by the source on startup when it is using source coordination. This method is meant to
     * explicitly say that source coordination should be used, since a source that implements {@link UsesSourceCoordination}
     * does not always use source coordination
     */
    void initialize();

    /**
     * This should be called by the source when it needs to get the next partition it should process on.
     * This method will attempt to acquire a partition for this instance of the source to work on, and if no partition is acquired, the partitionCreatorSupplier will be called to potentially create
     * new partitions. This method will then attempt to acquire a partition again after creating new partitions from the supplier, and will return the result of that attempt whether a partition was
     * acquired successfully or not. It is recommended to backoff and retry at the source level when this method returns an empty Optional.
     * @param partitionCreationSupplier - The Function that will provide partitions. This supplier will be called by the SourceCoordinator when no partitions are acquired
     *     for this instance of Data Prepper. This function will be passed a global state map, which will be empty until it is used and modified in the supplier function passed.
     *     If the global state map is not needed, then it can be ignored. Updating the global state map will save it, so the next time the supplier function is run,
     *     it will contain the most recent state from the previous supplier function run.
     * @return {@link SourcePartition} with the details about how to process this partition. Will repeatedly return the partition until
     * {@link SourceCoordinator#completePartition(String, Boolean)}
     * or {@link SourceCoordinator#closePartition(String, Duration, int, Boolean)} are called by the source,
     * or until the partition ownership times out.
     * @since 2.2
     */
    Optional<SourcePartition<T>> getNextPartition(final Function<Map<String, Object>, List<PartitionIdentifier>> partitionCreationSupplier);

    /**
     * Should be called by the source when it has fully processed a given partition
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException if the partition key could not be found in the distributed store
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException if the partition is not owned by this instance of SourceCoordinator
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException if the partition can not be completed
     * @param partitionKey - The partition key that uniquely identifies the partition of work that was fully processed
     * @param fromAcknowledgmentsCallback - Whether this method is being called from an acknowledgment callback or not
     * @since 2.2
     */
    void completePartition(final String partitionKey, final Boolean fromAcknowledgmentsCallback);

    /**
     * Should be called by the source when it has processed all that it can up to this point in time for a given partition,
     * but the partition is expected to need work done on it in the future. While not required, it is necessary in most cases to call saveStateForPartition before closing it.
     * Otherwise, when the partition reopens, the state will be outdated and duplicate parts of the partition will be processed.
     *
     * @param partitionKey - The partition key that uniquely identifies the partition of work
     * @param reopenAfter  - The duration from the current time to wait before this partition should be processed further at a later date
     * @param maxClosedCount - The number of times to allow this partition to be closed. Will mark the partition as completed if the partition has been closed this many times or more
     *                       in the past
     * @param fromAcknowledgmentsCallback - Whether this method is being called from an acknowledgment callback or not
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException if the partition key could not be found in the distributed store
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException if the partition is not owned by this instance of SourceCoordinator
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException if the partition can not be closed
     * @since 2.2
     */
    void closePartition(final String partitionKey, final Duration reopenAfter, final int maxClosedCount, final Boolean fromAcknowledgmentsCallback);

    /**
     * Should be called by the source when it has completed some work for a partition, and needs to save its progress before continuing work on the partition.
     * Should also be called before closePartition and giveUpPartitions if state needs to be updated.
     * Can optionally be called before completePartition if the progress state should be audited.
     *
     * @param <S>                    The partition state type
     * @param partitionKey           partition key that uniquely represents the partition
     * @param partitionProgressState The object to represent the latest partition progress state
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotFoundException if the partition key could not be found in the distributed store.
             Grab a new partition to process with {@link #getNextPartition(Function)} ()}
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionNotOwnedException if the partition is not owned by this instance of SourceCoordinator.
     *      Grab a new partition to process with {@link #getNextPartition(Function)} ()}
     *
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException if the partition state can not be saved
     * @since 2.2
     */
    <S extends T> void saveProgressStateForPartition(final String partitionKey, final S partitionProgressState);

    /**
     * Should be called by the source when it is shutting down to indicate that it will no longer be able to perform work on partitions,
     * or can be called to give up ownership of its partitions in order to pick up new ones with {@link #getNextPartition(Function)} ()}.
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException if the partition could not be given up due to some failure
     * @since 2.2
     */
    void giveUpPartitions();


    /**
     * Should be called by the source after when acknowledgments are enabled to keep ownership of the partition for acknowledgmentTimeout amount of time
     * before another instance of Data Prepper can pick it up for processing. Allows the source to acquire another partition immediately for processing
     * @param partitionKey - the partition to update for ack timeout
     * @param ackowledgmentTimeout - the amount of time that this partition can be completed by the acknowledgment callback before another instance of Data Prepper
     *                             can pick it up for processing
     */
    void updatePartitionForAcknowledgmentWait(final String partitionKey, final Duration ackowledgmentTimeout);
}
