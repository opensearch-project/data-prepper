/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ExportProgressState;

import java.time.Instant;
import java.util.Optional;

/**
 * An ExportPartition represents an export job needs to be run for tables.
 * Each export job has an export time associate with it.
 * Each job maintains the state such as total files/records etc. independently.
 * The source identifier contains keyword 'EXPORT'
 */
public class ExportPartition extends EnhancedSourcePartition<ExportProgressState> {
    public static final String PARTITION_TYPE = "EXPORT";

    private static final String DB_CLUSTER = "cluster";
    private static final String DB_INSTANCE = "instance";

    private final String dbIdentifier;

    private final boolean isCluster;

    private final ExportProgressState progressState;

    public ExportPartition(String dbIdentifier, boolean isCluster, ExportProgressState progressState) {
        this.dbIdentifier = dbIdentifier;
        this.isCluster = isCluster;
        this.progressState = progressState;
    }

    public ExportPartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        String [] keySplits = sourcePartitionStoreItem.getSourcePartitionKey().split("\\|");
        dbIdentifier = keySplits[0];
        isCluster = DB_CLUSTER.equals(keySplits[1]);
        progressState = convertStringToPartitionProgressState(ExportProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        final String dbType = isCluster ? DB_CLUSTER : DB_INSTANCE;
        return dbIdentifier + "|" + dbType;
    }

    @Override
    public Optional<ExportProgressState> getProgressState() {
        if (progressState != null) {
            return Optional.of(progressState);
        }
        return Optional.empty();
    }

    public String getDbIdentifier() {
        return dbIdentifier;
    }
}
