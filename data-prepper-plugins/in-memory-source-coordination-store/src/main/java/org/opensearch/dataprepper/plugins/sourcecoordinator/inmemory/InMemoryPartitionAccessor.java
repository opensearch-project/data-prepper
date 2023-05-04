/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.inmemory;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

public class InMemoryPartitionAccessor {

    private final Queue<String> queuedPartitions;
    private final Map<String, InMemorySourcePartitionStoreItem> partitionLookup;

    public InMemoryPartitionAccessor() {
        this.queuedPartitions = new LinkedList<>();
        this.partitionLookup = new HashMap<>();
    }

    public Optional<SourcePartitionStoreItem> getItem(final String partitionKey) {

        final InMemorySourcePartitionStoreItem item = partitionLookup.get(partitionKey);

        if (Objects.isNull(item)) {
            return Optional.empty();
        }

        return Optional.of(item);
    }

    public void queuePartition(final InMemorySourcePartitionStoreItem inMemorySourcePartitionStoreItem) {
        queuedPartitions.add(inMemorySourcePartitionStoreItem.getSourcePartitionKey());
        partitionLookup.put(inMemorySourcePartitionStoreItem.getSourcePartitionKey(), inMemorySourcePartitionStoreItem);
    }

    public Optional<SourcePartitionStoreItem> getNextItem() {

        final Set<String> seenPartitionKeys = new HashSet<>();
        while (!queuedPartitions.isEmpty()) {
            final String nextPartitionKey = queuedPartitions.peek();

            // Keep from having infinite loop (or infinite until a reopenAt timestamp is met) when only CLOSED partitions are left
            if (seenPartitionKeys.contains(nextPartitionKey)) {
                break;
            }
            seenPartitionKeys.add(nextPartitionKey);

            final InMemorySourcePartitionStoreItem nextItem = partitionLookup.get(nextPartitionKey);
            if (Objects.isNull(nextItem)) {
                queuedPartitions.remove();
            } else if (SourcePartitionStatus.CLOSED.equals(nextItem.getSourcePartitionStatus())
                            && Objects.nonNull(nextItem.getReOpenAt())
                            && nextItem.getReOpenAt().isAfter(Instant.now())) {
                // Put to the back of the queue when partition is closed and has not reached reopenAt instant
                queuedPartitions.remove();
                queuedPartitions.add(nextPartitionKey);
            } else {
                queuedPartitions.remove();
                return Optional.of(nextItem);
            }
        }

        return Optional.empty();
    }

    public void updateItem(final InMemorySourcePartitionStoreItem item) {
        final InMemorySourcePartitionStoreItem itemToUpdate = partitionLookup.get(item.getSourcePartitionKey());

        // If marking as unassigned or closed means the request is to process the next partition, so will requeue this one
        if (SourcePartitionStatus.UNASSIGNED.equals(item.getSourcePartitionStatus())
                || SourcePartitionStatus.CLOSED.equals(item.getSourcePartitionStatus())) {
            queuedPartitions.add(item.getSourcePartitionKey());
        }

        // The partition lookup map grows infinitely as more and more partitions are created
        // Should we remove COMPLETED partition items from memory and keep track of them with a Set of partitionKeys?
        // Will still work fine regardless if there aren't thousands of partitions, which seems ok since it is advised for non-production use

        if (Objects.nonNull(itemToUpdate)) {
            partitionLookup.put(item.getSourcePartitionKey(), item);
        }
    }
}
