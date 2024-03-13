/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.ExportProgressState;

import java.time.Instant;
import java.util.Optional;

/**
 * An ExportPartition represents an export job needs to be run for a table.
 * Each table may have multiple export jobs, each export job has an export time associate with it.
 * Each job maintains the state such as total files/records etc. independently.
 * The source identifier contains keyword 'EXPORT'
 */
public class ExportPartition extends EnhancedSourcePartition<ExportProgressState> {

    public static final String PARTITION_TYPE = "EXPORT";
    private final String collection;
    private final int partitionSize;

    private final Instant exportTime;

    private final ExportProgressState state;

    public ExportPartition(final SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        String[] keySplits = sourcePartitionStoreItem.getSourcePartitionKey().split("\\|");
        collection = keySplits[0];
        partitionSize = Integer.parseInt(keySplits[1]);
        exportTime = Instant.ofEpochMilli(Long.parseLong(keySplits[2]));
        this.state = convertStringToPartitionProgressState(ExportProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());

    }

    public ExportPartition(final String collection, final int partitionSize, final Instant exportTime,
                           final Optional<ExportProgressState> state) {
        this.collection = collection;
        this.partitionSize = partitionSize;
        this.exportTime = exportTime;
        this.state = state.orElse(null);

    }
    
    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return collection + "|" + partitionSize + "|" + exportTime.toEpochMilli();
    }

    @Override
    public Optional<ExportProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }


    public String getCollection() {
        return collection;
    }

    public int getPartitionSize() {
        return partitionSize;
    }

    public Instant getExportTime() {
        return exportTime;
    }


}
