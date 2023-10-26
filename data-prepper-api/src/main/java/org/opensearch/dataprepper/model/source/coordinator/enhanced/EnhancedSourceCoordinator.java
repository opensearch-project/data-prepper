/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source.coordinator.enhanced;

import java.time.Duration;
import java.util.Optional;

/**
 * A custom Lease based Coordinator Interface to enable distribution and coordination of work for the DynamoDB source
 * and to track progress and save state for these partitions of work.
 * <p>
 * Still trying to make this interface generic, considering that it may be merged with {@link org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator} in the future.
 * </p>
 * The major differences include:
 * <ul>
 *     <li>Support multiple types of partition</li>
 *     <li>Support multiple leases at the same time</li>
 * </ul>
 */
public interface EnhancedSourceCoordinator {

    /**
     * This method is used to create the partition item in the coordination store.
     *
     * @param partition A specific partition that extends {@link EnhancedSourcePartition}
     * @param <T>       The progress state class
     * @return True if partition is created successfully otherwise false.
     */
    <T> boolean createPartition(EnhancedSourcePartition<T> partition);


    /**
     * This method is used to acquire a lease on the partition item in the coordination store.
     *
     * @param partitionType The partition type identifier
     * @return A {@link EnhancedSourcePartition} instance
     */
    Optional<EnhancedSourcePartition> acquireAvailablePartition(String partitionType);

    /**
     * This method is used to update progress state for a partition in the coordination store.
     * It will also extend the timeout for ownership.
     *
     * @param partition The partition to be updated.
     * @param <T>       The progress state class
     * @param ownershipTimeoutRenewal The amount of time to update ownership of the partition before another instance can acquire it.
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException when the partition was not updated successfully
     */
    <T> void saveProgressStateForPartition(EnhancedSourcePartition<T> partition, Duration ownershipTimeoutRenewal);

    /**
     * This method is used to release the lease of a partition in the coordination store.
     * The progress state will also be updated.
     *
     * @param partition The partition to be updated.
     * @param <T>       The progress state class
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException when the partition was not updated successfully
     */
    <T> void giveUpPartition(EnhancedSourcePartition<T> partition);

    /**
     * This method is used to mark a partition as COMPLETED in the coordination store.
     * The progress state will also be updated.
     *
     * @param partition The partition to be updated.
     * @param <T>       The progress state class
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException when the partition was not updated successfully
     */
    <T> void completePartition(EnhancedSourcePartition<T> partition);

    /**
     * This method is used to mark a partition as CLOSED in the coordination store.
     * The closed partition will not be available for lease until reopen.
     * The progress state will also be updated.
     *
     * @param partition      The partition to be updated
     * @param reopenAfter    The duration from the current time to wait before this partition should be processed further at a later date
     * @param maxClosedCount The number of times to allow this partition to be closed. Will mark the partition as completed if the partition has been closed this many times or more
     *                       in the past
     * @param <T>            The progress state class
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException when the partition was not updated successfully
     */
    <T> void closePartition(EnhancedSourcePartition<T> partition, final Duration reopenAfter, final int maxClosedCount);


    /**
     * This method is used to get a partition from the coordination store.
     * Unlike acquiring, this does not add ownership to the item.
     * Hence, it's designed to be used as a "Global State" which can be read whenever needed.
     *
     * @param partitionKey A unique key for that partition
     * @return A {@link EnhancedSourcePartition} instance
     */
    Optional<EnhancedSourcePartition> getPartition(String partitionKey);

    /**
     * This method is to perform initialization for the coordinator
     */
    void initialize();

}
