/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.iceberg.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.state.WriteResultState;

import java.util.Optional;

public class WriteResultPartition extends EnhancedSourcePartition<WriteResultState> {

    public static final String PARTITION_TYPE = "WRITE_RESULT";

    private final String partitionKey;
    private final WriteResultState state;

    public WriteResultPartition(final String partitionKey, final WriteResultState state) {
        this.partitionKey = partitionKey;
        this.state = state;
    }

    public WriteResultPartition(final SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        this.partitionKey = sourcePartitionStoreItem.getSourcePartitionKey();
        this.state = convertStringToPartitionProgressState(
                WriteResultState.class, sourcePartitionStoreItem.getPartitionProgressState());
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
    public Optional<WriteResultState> getProgressState() {
        return Optional.of(state);
    }
}
