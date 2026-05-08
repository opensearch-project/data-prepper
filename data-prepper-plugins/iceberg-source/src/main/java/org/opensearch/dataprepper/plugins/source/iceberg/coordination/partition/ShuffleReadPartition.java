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
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ShuffleReadProgressState;

import java.util.Optional;

public class ShuffleReadPartition extends EnhancedSourcePartition<ShuffleReadProgressState> {

    public static final String PARTITION_TYPE = "SHUFFLE_READ";

    private final String partitionKey;
    private final ShuffleReadProgressState state;

    public ShuffleReadPartition(final String partitionKey, final ShuffleReadProgressState state) {
        this.partitionKey = partitionKey;
        this.state = state;
    }

    public ShuffleReadPartition(final SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        this.partitionKey = sourcePartitionStoreItem.getSourcePartitionKey();
        this.state = convertStringToPartitionProgressState(ShuffleReadProgressState.class,
                sourcePartitionStoreItem.getPartitionProgressState());
    }

    @Override
    public String getPartitionType() { return PARTITION_TYPE; }

    @Override
    public String getPartitionKey() { return partitionKey; }

    @Override
    public Optional<ShuffleReadProgressState> getProgressState() { return Optional.of(state); }
}
