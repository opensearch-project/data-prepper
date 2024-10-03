/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.saas.crawler.coordination;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.state.GlobalState;

import java.util.function.Function;

/**
 * Special partition factory just SAAS connector source.
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
            return new GlobalState(partitionStoreItem);
        }
    }


}
