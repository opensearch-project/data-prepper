/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.saas_crawler.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.saas_crawler.coordination.state.SaasWorkerProgressState;

import java.util.Optional;

/**
 * An SAAS source partition represents a chunk of work.
 * The source identifier contains keyword 'SAAS-WORKER'
 */
public class SaasSourcePartition extends EnhancedSourcePartition<SaasWorkerProgressState> {

    public static final String PARTITION_TYPE = "SAAS-WORKER";
    private final SaasWorkerProgressState state;
    private final String partitionKey;

    public SaasSourcePartition(final SourcePartitionStoreItem sourcePartitionStoreItem) {
        this.partitionKey = sourcePartitionStoreItem.getSourcePartitionKey();

        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        this.state = convertStringToPartitionProgressState(SaasWorkerProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());
    }

    public SaasSourcePartition(final SaasWorkerProgressState state,
                               String partitionKey) {
        this.state = state;
        this.partitionKey = partitionKey;
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return this.partitionKey;
    }

    @Override
    public Optional<SaasWorkerProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }

}
