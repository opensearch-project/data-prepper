/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.iceberg.coordination;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.partition.WriteResultPartition;

import java.util.function.Function;

public class PartitionFactory implements Function<SourcePartitionStoreItem, EnhancedSourcePartition> {

    @Override
    public EnhancedSourcePartition apply(final SourcePartitionStoreItem partitionStoreItem) {
        final String sourceIdentifier = partitionStoreItem.getSourceIdentifier();
        final String partitionType = sourceIdentifier.substring(sourceIdentifier.lastIndexOf('|') + 1);

        switch (partitionType) {
            case LeaderPartition.PARTITION_TYPE:
                return new LeaderPartition(partitionStoreItem);
            case WriteResultPartition.PARTITION_TYPE:
                return new WriteResultPartition(partitionStoreItem);
            default:
                throw new IllegalArgumentException("Unknown partition type: " + partitionType);
        }
    }
}
