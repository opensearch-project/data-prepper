/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;

import java.util.Map;
import java.util.Optional;

public class GlobalState extends EnhancedSourcePartition<Map<String, Object>> {

    private final String stateName;

    private Map<String, Object> state;

    public GlobalState(String stateName, Map<String, Object> state) {
        this.stateName = stateName;
        this.state = state;
    }

    public GlobalState(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        stateName = sourcePartitionStoreItem.getSourcePartitionKey();
        state = convertStringToPartitionProgressState(null, sourcePartitionStoreItem.getPartitionProgressState());
    }

    @Override
    public String getPartitionType() {
        return null;
    }

    @Override
    public String getPartitionKey() {
        return stateName;
    }

    @Override
    public Optional<Map<String, Object>> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }

    public void setProgressState(Map<String, Object> state) {
        this.state = state;
    }
}
