/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.StreamProgressState;

import java.util.Optional;

public class StreamPartition extends EnhancedSourcePartition<StreamProgressState> {

    public static final String PARTITION_TYPE = "STREAM";

    private final String dbIdentifier;
    private final StreamProgressState state;

    public StreamPartition(String dbIdentifier, StreamProgressState state) {
        this.dbIdentifier = dbIdentifier;
        this.state = state;
    }

    public StreamPartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        dbIdentifier = sourcePartitionStoreItem.getSourcePartitionKey();
        state = convertStringToPartitionProgressState(StreamProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return dbIdentifier;
    }

    @Override
    public Optional<StreamProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }
}
