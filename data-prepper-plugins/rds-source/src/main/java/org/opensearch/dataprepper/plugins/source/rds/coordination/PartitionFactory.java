/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.coordination;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.DataFilePartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.LeaderPartition;

import java.util.function.Function;

/**
 * Partition factory to map a {@link SourcePartitionStoreItem} to a {@link EnhancedSourcePartition}.
 */
public class PartitionFactory implements Function<SourcePartitionStoreItem, EnhancedSourcePartition> {

    @Override
    public EnhancedSourcePartition apply(SourcePartitionStoreItem partitionStoreItem) {
        String sourceIdentifier = partitionStoreItem.getSourceIdentifier();
        String partitionType = sourceIdentifier.substring(sourceIdentifier.lastIndexOf('|') + 1);

        if (LeaderPartition.PARTITION_TYPE.equals(partitionType)) {
            return new LeaderPartition(partitionStoreItem);
        } else if (ExportPartition.PARTITION_TYPE.equals(partitionType)) {
            return new ExportPartition(partitionStoreItem);
        } else if (DataFilePartition.PARTITION_TYPE.equals(partitionType)) {
            return new DataFilePartition(partitionStoreItem);
        } else {
            // Unable to acquire other partitions.
            return new GlobalState(partitionStoreItem);
        }
    }
}
