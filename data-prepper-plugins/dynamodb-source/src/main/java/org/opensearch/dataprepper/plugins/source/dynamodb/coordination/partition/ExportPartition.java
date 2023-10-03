/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.SourcePartition;
import org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state.ExportProgressState;

import java.time.Instant;
import java.util.Optional;

/**
 * An ExportPartition represents an export job needs to be run for a table.
 * Each table may have multiple export jobs, each export job has an export time associate with it.
 * Each job maintains the state such as total files/records etc. independently.
 * The source identifier contains keyword 'EXPORT'
 */
public class ExportPartition extends SourcePartition<ExportProgressState> {

    public static final String PARTITION_TYPE = "EXPORT";
    private final String tableArn;

    private final Instant exportTime;

    private final ExportProgressState state;

    public ExportPartition(SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        String[] keySplits = sourcePartitionStoreItem.getSourcePartitionKey().split("\\|");
        tableArn = keySplits[0];
        exportTime = Instant.ofEpochMilli(Long.valueOf(keySplits[1]));
        this.state = convertStringToPartitionProgressState(ExportProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());

    }

    public ExportPartition(String tableArn, Instant exportTime, Optional<ExportProgressState> state) {
        this.tableArn = tableArn;
        this.exportTime = exportTime;
        this.state = state.orElse(null);

    }
    
    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return tableArn + "|" + exportTime.toEpochMilli();
    }

    @Override
    public Optional<ExportProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }


    public String getTableArn() {
        return tableArn;
    }

    public Instant getExportTime() {
        return exportTime;
    }


}
