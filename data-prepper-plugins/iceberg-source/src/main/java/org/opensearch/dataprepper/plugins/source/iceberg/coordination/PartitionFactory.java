/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.coordination;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.ChangelogTaskPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.InitialLoadTaskPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.ShuffleReadPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.ShuffleWritePartition;

import java.util.function.Function;

public class PartitionFactory implements Function<SourcePartitionStoreItem, EnhancedSourcePartition> {

    @Override
    public EnhancedSourcePartition apply(final SourcePartitionStoreItem partitionStoreItem) {
        final String sourceIdentifier = partitionStoreItem.getSourceIdentifier();
        final String partitionType = sourceIdentifier.substring(sourceIdentifier.lastIndexOf('|') + 1);

        switch (partitionType) {
            case LeaderPartition.PARTITION_TYPE:
                return new LeaderPartition(partitionStoreItem);
            case ChangelogTaskPartition.PARTITION_TYPE:
                return new ChangelogTaskPartition(partitionStoreItem);
            case InitialLoadTaskPartition.PARTITION_TYPE:
                return new InitialLoadTaskPartition(partitionStoreItem);
            case ShuffleWritePartition.PARTITION_TYPE:
                return new ShuffleWritePartition(partitionStoreItem);
            case ShuffleReadPartition.PARTITION_TYPE:
                return new ShuffleReadPartition(partitionStoreItem);
            default:
                return new GlobalState(partitionStoreItem);
        }
    }
}
