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
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ShuffleWriteProgressState;

import java.util.Optional;

public class ShuffleWritePartition extends EnhancedSourcePartition<ShuffleWriteProgressState> {

    public static final String PARTITION_TYPE = "SHUFFLE_WRITE";

    private final String partitionKey;
    private final ShuffleWriteProgressState state;

    public ShuffleWritePartition(final String partitionKey, final ShuffleWriteProgressState state) {
        this.partitionKey = partitionKey;
        this.state = state;
    }

    public ShuffleWritePartition(final SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        this.partitionKey = sourcePartitionStoreItem.getSourcePartitionKey();
        this.state = convertStringToPartitionProgressState(ShuffleWriteProgressState.class,
                sourcePartitionStoreItem.getPartitionProgressState());
    }

    @Override
    public String getPartitionType() { return PARTITION_TYPE; }

    @Override
    public String getPartitionKey() { return partitionKey; }

    @Override
    public Optional<ShuffleWriteProgressState> getProgressState() { return Optional.of(state); }
}
