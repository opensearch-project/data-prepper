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
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ResyncPartition;

import java.util.function.Function;

/**
 * Partition factory to map a {@link SourcePartitionStoreItem} to a {@link EnhancedSourcePartition}.
 */
public class PartitionFactory implements Function<SourcePartitionStoreItem, EnhancedSourcePartition> {

    @Override
    public EnhancedSourcePartition apply(SourcePartitionStoreItem partitionStoreItem) {
        String sourceIdentifier = partitionStoreItem.getSourceIdentifier();
        String partitionType = sourceIdentifier.substring(sourceIdentifier.lastIndexOf('|') + 1);

        switch (partitionType) {
            case LeaderPartition.PARTITION_TYPE:
                return new LeaderPartition(partitionStoreItem);
            case ExportPartition.PARTITION_TYPE:
                return new ExportPartition(partitionStoreItem);
            case DataFilePartition.PARTITION_TYPE:
                return new DataFilePartition(partitionStoreItem);
            case StreamPartition.PARTITION_TYPE:
                return new StreamPartition(partitionStoreItem);
            case ResyncPartition.PARTITION_TYPE:
                return new ResyncPartition(partitionStoreItem);
            default:
                // Unable to acquire other partitions.
                return new GlobalState(partitionStoreItem);
        }
    }
}
