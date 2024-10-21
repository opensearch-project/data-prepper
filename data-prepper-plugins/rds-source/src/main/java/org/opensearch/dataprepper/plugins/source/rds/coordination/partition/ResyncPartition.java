/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ResyncProgressState;

import java.util.Optional;

public class ResyncPartition extends EnhancedSourcePartition<ResyncProgressState> {

    public static final String PARTITION_TYPE = "RESYNC";

    private final String database;
    private final String table;
    private final long timestamp;
    private final ResyncProgressState state;

    public ResyncPartition(String database, String table, long timestamp, ResyncProgressState state) {
        this.database = database;
        this.table = table;
        this.timestamp = timestamp;
        this.state = state;
    }

    public ResyncPartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        String[] keySplits = sourcePartitionStoreItem.getSourcePartitionKey().split("\\|");
        database = keySplits[0];
        table = keySplits[1];
        timestamp = Long.parseLong(keySplits[2]);
        state = convertStringToPartitionProgressState(ResyncProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return database + "|" + table + "|" + timestamp;
    }

    @Override
    public Optional<ResyncProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }
}
