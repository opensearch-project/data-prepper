/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.partition;

import lombok.Getter;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ResyncProgressState;

import java.util.Optional;

public class ResyncPartition extends EnhancedSourcePartition<ResyncProgressState> {

    public static final String PARTITION_TYPE = "RESYNC";

    private final String database;
    private final String table;
    private final long timestamp;
    private final PartitionKeyInfo partitionKeyInfo;
    private final ResyncProgressState state;

    public ResyncPartition(String database, String table, long timestamp, ResyncProgressState state) {
        this.database = database;
        this.table = table;
        this.timestamp = timestamp;
        partitionKeyInfo = new PartitionKeyInfo(database, table, timestamp);
        this.state = state;
    }

    public ResyncPartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        partitionKeyInfo = PartitionKeyInfo.fromString(sourcePartitionStoreItem.getSourcePartitionKey());
        database = partitionKeyInfo.getDatabase();
        table = partitionKeyInfo.getTable();
        timestamp = partitionKeyInfo.getTimestamp();
        state = convertStringToPartitionProgressState(ResyncProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return partitionKeyInfo.toString();
    }

    @Override
    public Optional<ResyncProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }

    public PartitionKeyInfo getPartitionKeyInfo() {
        return partitionKeyInfo;
    }

    @Getter
    public static class PartitionKeyInfo {
        private final String database;
        private final String table;
        private final long timestamp;

        private PartitionKeyInfo(String database, String table, long timestamp) {
            this.database = database;
            this.table = table;
            this.timestamp = timestamp;
        }

        private static PartitionKeyInfo fromString(String partitionKey) {
            String[] keySplits = partitionKey.split("\\|");
            if (keySplits.length != 3) {
                throw new IllegalArgumentException("Invalid partition key: " + partitionKey);
            }
            return new PartitionKeyInfo(keySplits[0], keySplits[1], Long.parseLong(keySplits[2]));
        }

        @Override
        public String toString() {
            return database + "|" + table + "|" + timestamp;
        }
    }
}
