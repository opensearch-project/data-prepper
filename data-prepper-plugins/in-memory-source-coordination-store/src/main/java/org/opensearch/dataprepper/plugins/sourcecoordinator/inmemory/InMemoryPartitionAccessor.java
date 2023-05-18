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

    private final Queue<QueuedPartitionsItem> unassignedPartitions;
    private final Queue<QueuedPartitionsItem> closedPartitions;
    private final Set<String> completedPartitions;
    private final Map<String, Map<String, InMemorySourcePartitionStoreItem>> partitionLookup;

    public InMemoryPartitionAccessor() {
        this.unassignedPartitions = new PriorityQueue<>();
        this.closedPartitions = new PriorityQueue<>();
        this.completedPartitions = new HashSet<>();
        this.partitionLookup = new HashMap<>();
    }

    public Optional<SourcePartitionStoreItem> getItem(final String sourceIdentifier, final String partitionKey) {

        if (!partitionLookup.containsKey(sourceIdentifier)) {
            return Optional.empty();
        }

        if (completedPartitions.contains(sourceIdentifier + ":" + partitionKey)) {
            final InMemorySourcePartitionStoreItem placeHolderItem = new InMemorySourcePartitionStoreItem();
            placeHolderItem.setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
            placeHolderItem.setSourceIdentifier(sourceIdentifier);
            placeHolderItem.setSourcePartitionKey(partitionKey);

            return Optional.of(placeHolderItem);
        }

        final InMemorySourcePartitionStoreItem item = partitionLookup.get(sourceIdentifier).get(partitionKey);

        return Optional.ofNullable(item);
    }

    public void queuePartition(final InMemorySourcePartitionStoreItem inMemorySourcePartitionStoreItem) {

        final Map<String, InMemorySourcePartitionStoreItem> partitionMap = partitionLookup.get(inMemorySourcePartitionStoreItem.getSourceIdentifier());

        if (Objects.isNull(partitionMap)) {
            final Map<String, InMemorySourcePartitionStoreItem> newPartitionMap = new HashMap<>();
            newPartitionMap.put(inMemorySourcePartitionStoreItem.getSourcePartitionKey(), inMemorySourcePartitionStoreItem);
            partitionLookup.put(inMemorySourcePartitionStoreItem.getSourceIdentifier(), newPartitionMap);
            queuePartitionItem(inMemorySourcePartitionStoreItem);
            return;
        }

        partitionMap.put(inMemorySourcePartitionStoreItem.getSourcePartitionKey(), inMemorySourcePartitionStoreItem);
        queuePartitionItem(inMemorySourcePartitionStoreItem);
    }

    public Optional<SourcePartitionStoreItem> getNextItem() {
        final QueuedPartitionsItem nextClosedPartitionItem = closedPartitions.peek();

        if (Objects.nonNull(nextClosedPartitionItem)) {
            if (nextClosedPartitionItem.sortedTimestamp.isBefore(Instant.now()) && partitionLookup.containsKey(nextClosedPartitionItem.sourceIdentifier)) {
                closedPartitions.remove();
                return Optional.ofNullable(partitionLookup.get(nextClosedPartitionItem.sourceIdentifier).get(nextClosedPartitionItem.partitionKey));
            }
        }

        final QueuedPartitionsItem nextUnassignedPartitionItem = unassignedPartitions.peek();

        if (Objects.nonNull(nextUnassignedPartitionItem)) {
            if (partitionLookup.containsKey(nextUnassignedPartitionItem.sourceIdentifier)) {
                unassignedPartitions.remove();
                return Optional.ofNullable(partitionLookup.get(nextUnassignedPartitionItem.sourceIdentifier).get(nextUnassignedPartitionItem.partitionKey));
            }
        }

        return Optional.empty();
    }

    public void updateItem(final InMemorySourcePartitionStoreItem item) {

        if (!partitionLookup.containsKey(item.getSourceIdentifier()) || completedPartitions.contains(
                item.getSourceIdentifier() + ":" + item.getSourcePartitionKey())) {
            return;
        }

        if (SourcePartitionStatus.COMPLETED.equals(item.getSourcePartitionStatus())) {
            completedPartitions.add(item.getSourceIdentifier() + ":" + item.getSourcePartitionKey());
            partitionLookup.get(item.getSourceIdentifier()).remove(item.getSourcePartitionKey());
            return;
        }

        // If marking as unassigned or closed means the request is to process the next partition, so will requeue this one
        queuePartitionItem(item);

        partitionLookup.get(item.getSourceIdentifier()).put(item.getSourcePartitionKey(), item);
    }

    private void queuePartitionItem(final InMemorySourcePartitionStoreItem inMemorySourcePartitionStoreItem) {
        if (SourcePartitionStatus.UNASSIGNED.equals(inMemorySourcePartitionStoreItem.getSourcePartitionStatus())) {
            unassignedPartitions.add(new QueuedPartitionsItem(inMemorySourcePartitionStoreItem.getSourceIdentifier(),
                    inMemorySourcePartitionStoreItem.getSourcePartitionKey(), Instant.now()));
        } else if (SourcePartitionStatus.CLOSED.equals(inMemorySourcePartitionStoreItem.getSourcePartitionStatus())) {
            closedPartitions.add(new QueuedPartitionsItem(inMemorySourcePartitionStoreItem.getSourceIdentifier(),
                    inMemorySourcePartitionStoreItem.getSourcePartitionKey(), inMemorySourcePartitionStoreItem.getReOpenAt()));
        }
    }

    protected static class QueuedPartitionsItem implements Comparable<QueuedPartitionsItem> {

        private final String sourceIdentifier;
        private final String partitionKey;
        private final Instant sortedTimestamp;

        public QueuedPartitionsItem(final String sourceIdentifier, final String partitionKey, final Instant sortedTimestamp) {
            this.sourceIdentifier = sourceIdentifier;
            this.partitionKey = partitionKey;
            this.sortedTimestamp = sortedTimestamp;
        }

        @Override
        public int compareTo(final QueuedPartitionsItem o) {
            if (Objects.isNull(this.sortedTimestamp) && Objects.isNull(o.sortedTimestamp)) {
                return 0;
            }

            if (Objects.isNull(this.sortedTimestamp)) {
                return -1;
            }

            if (Objects.isNull(o.sortedTimestamp)) {
                return 1;
            }

            return this.sortedTimestamp.compareTo(o.sortedTimestamp);
        }
    }
}
