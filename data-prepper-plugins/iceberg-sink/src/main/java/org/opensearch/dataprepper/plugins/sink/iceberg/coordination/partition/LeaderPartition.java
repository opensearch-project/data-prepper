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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class LeaderPartition extends EnhancedSourcePartition<Map<String, Object>> {

    public static final String PARTITION_TYPE = "LEADER";
    private static final String DEFAULT_PARTITION_KEY = "GLOBAL";

    private final Map<String, Object> state;

    public LeaderPartition() {
        this.state = Collections.emptyMap();
    }

    public LeaderPartition(final SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        this.state = convertStringToPartitionProgressState(
                null, sourcePartitionStoreItem.getPartitionProgressState());
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return DEFAULT_PARTITION_KEY;
    }

    @Override
    public Optional<Map<String, Object>> getProgressState() {
        return Optional.of(state);
    }
}
