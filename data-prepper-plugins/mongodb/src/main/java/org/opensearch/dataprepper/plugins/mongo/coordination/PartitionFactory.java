/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.coordination;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.S3FolderPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.StreamPartition;

import java.util.function.Function;

/**
 *  Partition factory for MongoDB/DocumentDB source.
 */
public class PartitionFactory implements Function<SourcePartitionStoreItem, EnhancedSourcePartition> {


    @Override
    public EnhancedSourcePartition apply(final SourcePartitionStoreItem partitionStoreItem) {
        String sourceIdentifier = partitionStoreItem.getSourceIdentifier();
        String partitionType = sourceIdentifier.substring(sourceIdentifier.lastIndexOf('|') + 1);

        switch (partitionType) {
            case ExportPartition.PARTITION_TYPE:
                return new ExportPartition(partitionStoreItem);
            case DataQueryPartition.PARTITION_TYPE:
                return new DataQueryPartition(partitionStoreItem);
            case StreamPartition.PARTITION_TYPE:
                return new StreamPartition(partitionStoreItem);
            case LeaderPartition.PARTITION_TYPE:
                return new LeaderPartition(partitionStoreItem);
            case S3FolderPartition.PARTITION_TYPE:
                return new S3FolderPartition(partitionStoreItem);
            default:
                // Unable to acquire other partitions.
                return new GlobalState(partitionStoreItem);
        }
    }


}
