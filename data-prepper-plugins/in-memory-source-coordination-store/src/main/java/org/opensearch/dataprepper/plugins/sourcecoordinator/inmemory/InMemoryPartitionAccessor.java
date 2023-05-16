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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

public class InMemoryPartitionAccessor {

    private final Queue<QueuedPartitionsItem> queuedPartitions;
    private final Map<String, InMemorySourcePartitionStoreItem> partitionLookup;

    public InMemoryPartitionAccessor() {
        this.queuedPartitions = new PriorityQueue<>();
        this.partitionLookup = new HashMap<>();
    }

    public Optional<SourcePartitionStoreItem> getItem(final String partitionKey) {

        final InMemorySourcePartitionStoreItem item = partitionLookup.get(partitionKey);

        return Optional.ofNullable(item);
    }

    public void queuePartition(final InMemorySourcePartitionStoreItem inMemorySourcePartitionStoreItem) {
        queuedPartitions.add(new QueuedPartitionsItem(inMemorySourcePartitionStoreItem.getSourcePartitionKey(), inMemorySourcePartitionStoreItem.getReOpenAt()));
        partitionLookup.put(inMemorySourcePartitionStoreItem.getSourcePartitionKey(), inMemorySourcePartitionStoreItem);
    }

    public Optional<SourcePartitionStoreItem> getNextItem() {

        final Set<String> seenPartitionKeys = new HashSet<>();
        while (!queuedPartitions.isEmpty()) {
            final QueuedPartitionsItem nextPartitionItem = queuedPartitions.peek();

            // Keep from having infinite loop (or infinite until a reopenAt timestamp is met) when only CLOSED partitions are left
            if (seenPartitionKeys.contains(nextPartitionItem.partitionKey)) {
                break;
            }
            seenPartitionKeys.add(nextPartitionItem.partitionKey);

            final InMemorySourcePartitionStoreItem nextItem = partitionLookup.get(nextPartitionItem.partitionKey);
            if (Objects.isNull(nextItem)) {
                queuedPartitions.remove();
            } else if (SourcePartitionStatus.CLOSED.equals(nextItem.getSourcePartitionStatus())
                            && Objects.nonNull(nextItem.getReOpenAt())
                            && nextItem.getReOpenAt().isAfter(Instant.now())) {
                // Put back into the queue when partition is closed and has not reached reopenAt instant
                queuedPartitions.remove();
                queuedPartitions.add(nextPartitionItem);
            } else {
                queuedPartitions.remove();
                return Optional.of(nextItem);
            }
        }

        return Optional.empty();
    }

    public void updateItem(final InMemorySourcePartitionStoreItem item) {

        // If marking as unassigned or closed means the request is to process the next partition, so will requeue this one
        if (SourcePartitionStatus.UNASSIGNED.equals(item.getSourcePartitionStatus())
                || SourcePartitionStatus.CLOSED.equals(item.getSourcePartitionStatus())) {
            queuedPartitions.add(new QueuedPartitionsItem(item.getSourcePartitionKey(), item.getReOpenAt()));
        }

        // The partition lookup map grows infinitely as more and more partitions are created
        // I don't think there's a great solution to this, but seems ok since it is advised for non-production use

        final InMemorySourcePartitionStoreItem itemToUpdate = partitionLookup.get(item.getSourcePartitionKey());

        if (Objects.nonNull(itemToUpdate)) {
            partitionLookup.put(item.getSourcePartitionKey(), item);
        }
    }

    protected static class QueuedPartitionsItem implements Comparable<QueuedPartitionsItem> {

        private final String partitionKey;
        private final Instant reOpenAt;

        public QueuedPartitionsItem(final String partitionKey, final Instant reOpenAt) {
            this.partitionKey = partitionKey;
            this.reOpenAt = reOpenAt;
        }

        @Override
        public int compareTo(final QueuedPartitionsItem o) {
            if (Objects.isNull(this.reOpenAt) && Objects.isNull(o.reOpenAt)) {
                return 0;
            }

            if (Objects.isNull(this.reOpenAt)) {
                return -1;
            }

            if (Objects.isNull(o.reOpenAt)) {
                return 1;
            }

            return this.reOpenAt.compareTo(o.reOpenAt);
        }
    }
}
