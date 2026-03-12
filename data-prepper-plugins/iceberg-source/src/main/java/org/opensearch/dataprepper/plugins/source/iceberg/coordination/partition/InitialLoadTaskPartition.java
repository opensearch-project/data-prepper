/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.InitialLoadTaskProgressState;

import java.util.Optional;

public class InitialLoadTaskPartition extends EnhancedSourcePartition<InitialLoadTaskProgressState> {

    public static final String PARTITION_TYPE = "INITIAL_LOAD_TASK";

    private final String partitionKey;
    private final InitialLoadTaskProgressState state;

    public InitialLoadTaskPartition(final String partitionKey, final InitialLoadTaskProgressState state) {
        this.partitionKey = partitionKey;
        this.state = state;
    }

    public InitialLoadTaskPartition(final SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        this.partitionKey = sourcePartitionStoreItem.getSourcePartitionKey();
        this.state = convertStringToPartitionProgressState(
                InitialLoadTaskProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return partitionKey;
    }

    @Override
    public Optional<InitialLoadTaskProgressState> getProgressState() {
        return Optional.of(state);
    }
}
