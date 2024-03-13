/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.StreamProgressState;

import java.util.Optional;

public class StreamPartition extends EnhancedSourcePartition<StreamProgressState> {

    public static final String PARTITION_TYPE = "STREAM";

    private final String collection;

    private final StreamProgressState state;

    public StreamPartition(String collection, Optional<StreamProgressState> state) {
        this.collection = collection;
        this.state = state.orElse(null);
    }

    public StreamPartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        String[] keySplits = sourcePartitionStoreItem.getSourcePartitionKey().split("\\|");
        collection = keySplits[0];
        this.state = convertStringToPartitionProgressState(StreamProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());

    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return collection;
    }

    @Override
    public Optional<StreamProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }

    public String getCollection() {
        return collection;
    }
}
