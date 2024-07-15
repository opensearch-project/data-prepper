/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.DataFileProgressState;

import java.util.Optional;

/**
 * An DataFilePartition represents an export data file needs to be loaded.
 * The source identifier contains keyword 'DATAFILE'
 */
public class DataFilePartition extends EnhancedSourcePartition<DataFileProgressState> {

    public static final String PARTITION_TYPE = "DATAFILE";

    private final String exportTaskId;
    private final String bucket;
    private final String key;
    private final DataFileProgressState state;

    public DataFilePartition(final SourcePartitionStoreItem sourcePartitionStoreItem) {

        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        String[] keySplits = sourcePartitionStoreItem.getSourcePartitionKey().split("\\|");
        exportTaskId = keySplits[0];
        bucket = keySplits[1];
        key = keySplits[2];
        state = convertStringToPartitionProgressState(DataFileProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());

    }

    public DataFilePartition(final String exportTaskId,
                             final String bucket,
                             final String key,
                             final Optional<DataFileProgressState> state) {
        this.exportTaskId = exportTaskId;
        this.bucket = bucket;
        this.key = key;
        this.state = state.orElse(null);
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return exportTaskId + "|" + bucket + "|" + key;
    }

    @Override
    public Optional<DataFileProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }

    public String getExportTaskId() {
        return exportTaskId;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }
}
