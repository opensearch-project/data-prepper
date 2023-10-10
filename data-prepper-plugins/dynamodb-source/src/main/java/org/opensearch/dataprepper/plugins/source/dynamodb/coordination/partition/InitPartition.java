/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.SourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.EmptyProgressState;

import java.util.Optional;

/**
 * Use a partition to track whether initialization has happened or not.
 * This is to ensure that initialization is triggered once.
 * The downside is that after initialization, changes to configuration will be ignored.
 * The source identifier contains keyword 'INIT'
 */
public class InitPartition extends SourcePartition<EmptyProgressState> {
    public static final String PARTITION_TYPE = "INIT";

    private static final String DEFAULT_PARTITION_KEY = "GLOBAL";

    public InitPartition() {
    }

    public InitPartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
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
    public Optional<EmptyProgressState> getProgressState() {
        return Optional.empty();
    }
}
