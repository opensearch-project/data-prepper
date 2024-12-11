/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.neptune.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.state.StreamProgressState;

import java.util.Optional;

public class StreamPartition extends EnhancedSourcePartition<StreamProgressState> {

    public static final String PARTITION_TYPE = "STREAM";

    private final StreamProgressState state;

    public StreamPartition(final StreamProgressState state) {
        this.state = state;
    }

    public StreamPartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        String[] keySplits = sourcePartitionStoreItem.getSourcePartitionKey().split("\\|");
        this.state = convertStringToPartitionProgressState(StreamProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());
    }

    @Override
    public String getPartitionKey() {
        return "neptune"; // FIXME
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public Optional<StreamProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }
}
