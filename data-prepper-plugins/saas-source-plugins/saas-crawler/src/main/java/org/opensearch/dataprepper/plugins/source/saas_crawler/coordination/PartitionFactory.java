/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.saas_crawler.coordination;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.saas_crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.saas_crawler.coordination.partition.SaasSourcePartition;

import java.util.function.Function;

/**
 * Partition factory for SAAS source plugins.
 */
public class PartitionFactory implements Function<SourcePartitionStoreItem, EnhancedSourcePartition> {


    @Override
    public EnhancedSourcePartition apply(SourcePartitionStoreItem partitionStoreItem) {
        String sourceIdentifier = partitionStoreItem.getSourceIdentifier();
        String partitionType = sourceIdentifier.substring(sourceIdentifier.lastIndexOf('|') + 1);

         if (LeaderPartition.PARTITION_TYPE.equals(partitionType)) {
            return new LeaderPartition(partitionStoreItem);
        } else if (SaasSourcePartition.PARTITION_TYPE.equals(partitionType)) {
            return new SaasSourcePartition(partitionStoreItem);
        } else {
            // Unable to acquire other partitions.
            // Probably we will introduce Global state in the future but for now, we don't expect to reach here.
             throw new RuntimeException(String.format("Unable to acquire other partition : %s. " +
                     "Probably we will introduce Global state in the future but for now, " +
                     "we don't expect to reach here.", partitionType));
        }
    }


}
