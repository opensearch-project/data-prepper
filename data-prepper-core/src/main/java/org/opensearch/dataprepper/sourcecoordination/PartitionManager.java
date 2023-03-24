/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination;

import org.opensearch.dataprepper.model.source.SourcePartition;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

public class PartitionManager {

    private final Queue<SourcePartition> partitionsToProcess;
    private final Set<String> ownedPartitionKeys;

    public PartitionManager() {
        this.partitionsToProcess = new LinkedList<>();
        this.ownedPartitionKeys = new HashSet<>();
    }

    public Optional<SourcePartition> getNextPartition() {
        while (Objects.nonNull(partitionsToProcess.peek()) && !ownedPartitionKeys.contains(partitionsToProcess.peek().getPartitionKey())) {
            partitionsToProcess.remove();
        }

        if (Objects.isNull(partitionsToProcess.peek())) {
            return Optional.empty();
        }

        return Optional.of(partitionsToProcess.peek());
    }

    public void clearOwnedPartitions() {
        ownedPartitionKeys.clear();
    }

    public void removePartition(final String partitionKey) {
        ownedPartitionKeys.remove(partitionKey);
    }

    public void queuePartition(final SourcePartition sourcePartition) {
        ownedPartitionKeys.add(sourcePartition.getPartitionKey());
        partitionsToProcess.add(sourcePartition);
    }
}
