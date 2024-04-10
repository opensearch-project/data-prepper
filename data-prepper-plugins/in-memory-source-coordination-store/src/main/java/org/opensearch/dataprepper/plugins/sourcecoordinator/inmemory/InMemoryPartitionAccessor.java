/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.inmemory;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

public class InMemoryPartitionAccessor {

    static final String GLOBAL_STATE_ITEM_SUFFIX = "GLOBAL_STATE";
    private static final String SOURCE_IDENTIFIER_PARTITION_KEY_COMBINATION = "%s|%s";

    private final Queue<QueuedPartitionsItem> unassignedPartitions;
    private final Queue<QueuedPartitionsItem> closedPartitions;
    private final Set<String> completedPartitions;
    private final Map<String, Map<String, InMemorySourcePartitionStoreItem>> partitionLookup;
    private final Map<String, InMemorySourcePartitionStoreItem> globalStateItems;

    public InMemoryPartitionAccessor() {
        this.unassignedPartitions = new PriorityQueue<>();
        this.closedPartitions = new PriorityQueue<>();
        this.completedPartitions = new HashSet<>();
        this.partitionLookup = new HashMap<>();
        this.globalStateItems = new HashMap<>();
    }

    public Optional<SourcePartitionStoreItem> getItem(final String sourceIdentifier, final String partitionKey) {
        final String sourceIdentifierPartitionKey = String.format(SOURCE_IDENTIFIER_PARTITION_KEY_COMBINATION, sourceIdentifier, partitionKey);

        if (isGlobalStateItem(sourceIdentifier)) {
            return Optional.ofNullable(globalStateItems.get(sourceIdentifierPartitionKey));
        }

        if (!partitionLookup.containsKey(sourceIdentifier)) {
            return Optional.empty();
        }

        if (completedPartitions.contains(sourceIdentifierPartitionKey)) {
            final InMemorySourcePartitionStoreItem placeHolderItem = new InMemorySourcePartitionStoreItem();
            placeHolderItem.setSourcePartitionStatus(SourcePartitionStatus.COMPLETED);
            placeHolderItem.setSourceIdentifier(sourceIdentifier);
            placeHolderItem.setSourcePartitionKey(partitionKey);

            return Optional.of(placeHolderItem);
        }

        final InMemorySourcePartitionStoreItem item = partitionLookup.get(sourceIdentifier).get(partitionKey);

        return Optional.ofNullable(item);
    }

    public List<SourcePartitionStoreItem> getAllItem(final String sourceIdentifier) {
        if (!partitionLookup.containsKey(sourceIdentifier)) {
            return Collections.emptyList();
        }

        return new ArrayList<>(partitionLookup.get(sourceIdentifier).values());
    }

    public void queuePartition(final InMemorySourcePartitionStoreItem inMemorySourcePartitionStoreItem) {
        if (isGlobalStateItem(inMemorySourcePartitionStoreItem.getSourceIdentifier())) {
            globalStateItems.put(String.format(SOURCE_IDENTIFIER_PARTITION_KEY_COMBINATION,
                            inMemorySourcePartitionStoreItem.getSourceIdentifier(), inMemorySourcePartitionStoreItem.getSourcePartitionKey()),
                    inMemorySourcePartitionStoreItem);
            return;
        }

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
        final QueuedPartitionsItem nextUnassignedPartitionItem = unassignedPartitions.peek();

        if (Objects.nonNull(nextUnassignedPartitionItem)) {
            if (partitionLookup.containsKey(nextUnassignedPartitionItem.sourceIdentifier)) {
                unassignedPartitions.remove();
                return Optional.ofNullable(partitionLookup.get(nextUnassignedPartitionItem.sourceIdentifier).get(nextUnassignedPartitionItem.partitionKey));
            }
        }

        final QueuedPartitionsItem nextClosedPartitionItem = closedPartitions.peek();

        if (Objects.nonNull(nextClosedPartitionItem)) {
            if (nextClosedPartitionItem.sortedTimestamp.isBefore(Instant.now()) && partitionLookup.containsKey(nextClosedPartitionItem.sourceIdentifier)) {
                closedPartitions.remove();
                return Optional.ofNullable(partitionLookup.get(nextClosedPartitionItem.sourceIdentifier).get(nextClosedPartitionItem.partitionKey));
            }
        }

        return Optional.empty();
    }

    public void updateItem(final InMemorySourcePartitionStoreItem item) {
        final String sourceIdentifierPartitionKey = String.format(SOURCE_IDENTIFIER_PARTITION_KEY_COMBINATION, item.getSourceIdentifier(), item.getSourcePartitionKey());

        if (isGlobalStateItem(item.getSourceIdentifier())) {
            globalStateItems.put(sourceIdentifierPartitionKey, item);
            return;
        }

        if (!partitionLookup.containsKey(item.getSourceIdentifier()) || completedPartitions.contains(sourceIdentifierPartitionKey)) {
            return;
        }

        if (SourcePartitionStatus.COMPLETED.equals(item.getSourcePartitionStatus())) {
            completedPartitions.add(sourceIdentifierPartitionKey);
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

    private boolean isGlobalStateItem(final String sourceIdentifier) {
        return sourceIdentifier.endsWith(GLOBAL_STATE_ITEM_SUFFIX);
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
