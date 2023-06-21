/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.source;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import java.time.Duration;
import java.util.Optional;

/**
 * The interface to be implemented when creating a new store plugin for a {@link org.opensearch.dataprepper.model.source.coordinator.SourceCoordinator} to use
 * to coordinate and save progress for processing source partitions
 * @since 2.2
 */
public interface SourceCoordinationStore {

    void initializeStore();

    Optional<SourcePartitionStoreItem> getSourcePartitionItem(final String sourceIdentifier, final String sourcePartitionKey);

    boolean tryCreatePartitionItem(final String sourceIdentifier,
                                   final String partitionKey,
                                   final SourcePartitionStatus sourcePartitionStatus,
                                   final Long closedCount,
                                   final String partitionProgressState);

    /**
     * The following scenarios should qualify a partition as available to be acquired
     * 1. The partition status is UNASSIGNED
     * 2. The partition status is CLOSED and the reOpenAt timestamp has passed
     * 3. The partition status is ASSIGNED and the partitionOwnershipTimeout has passed
     * @param sourceIdentifier - The identifier for the source
     * @param ownerId - The unique owner id for a sub-pipeline
     * @param ownershipTimeout The amount of time before the ownership of the acquired partition expires
     * @return The partition that was acquired successfully. Empty if no partition could be acquired.
     */
    Optional<SourcePartitionStoreItem> tryAcquireAvailablePartition(final String sourceIdentifier, final String ownerId, final Duration ownershipTimeout);

    /**
     * This method attempts to update the partition item to the desired state
     * @throws org.opensearch.dataprepper.model.source.coordinator.exceptions.PartitionUpdateException when the partition was not updated successfully
     * @param updateItem - The item to update in the source coordination store
     */
    void tryUpdateSourcePartitionItem(final SourcePartitionStoreItem updateItem);
}
