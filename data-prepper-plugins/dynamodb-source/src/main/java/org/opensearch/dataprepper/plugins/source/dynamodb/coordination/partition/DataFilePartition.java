/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.DataFileProgressState;

import java.util.Optional;

/**
 * An DataFilePartition represents an export data file needs to be loaded.
 * The source identifier contains keyword 'DATAFILE'
 */
public class DataFilePartition extends EnhancedSourcePartition<DataFileProgressState> {

    public static final String PARTITION_TYPE = "DATAFILE";

    private final String exportArn;
    private final String bucket;
    private final String key;

    private DataFileProgressState state;

    public DataFilePartition(SourcePartitionStoreItem sourcePartitionStoreItem) {

        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        String[] keySplits = sourcePartitionStoreItem.getSourcePartitionKey().split("\\|");
        exportArn = keySplits[0];
        bucket = keySplits[1];
        key = keySplits[2];
        this.state = convertStringToPartitionProgressState(DataFileProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());

    }

    public DataFilePartition(String exportArn, String bucket, String key, Optional<DataFileProgressState> state) {
        this.exportArn = exportArn;
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
        return exportArn + "|" + bucket + "|" + key;
    }

    @Override
    public Optional<DataFileProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }

    public String getExportArn() {
        return exportArn;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }
}
