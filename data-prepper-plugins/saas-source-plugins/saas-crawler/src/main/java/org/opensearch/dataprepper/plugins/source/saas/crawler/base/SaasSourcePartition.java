/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.saas.crawler.base;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * An SAAS source partition represents a chunk of work.
 * The source identifier contains keyword 'SAAS-WORKER'
 */
public class SaasSourcePartition extends EnhancedSourcePartition<SaasWorkerProgressState> {

    public static final String PARTITION_TYPE = "SAAS-WORKER";
    private final SaasWorkerProgressState state;
    private final String sourceName;
    private final String projectName;
    private final String issueType;

    public SaasSourcePartition(final SourcePartitionStoreItem sourcePartitionStoreItem) {
        String partitionKey = sourcePartitionStoreItem.getSourcePartitionKey();
        String[] parts = partitionKey.split("|");
        this.sourceName = parts[0];
        this.projectName = parts[1];
        this.issueType = parts[2];
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        String[] keySplits = sourcePartitionStoreItem.getSourcePartitionKey().split("\\|");
        this.state = convertStringToPartitionProgressState(SaasWorkerProgressState.class, sourcePartitionStoreItem.getPartitionProgressState());
    }

    public SaasSourcePartition(final Optional<SaasWorkerProgressState> state,
                               String sourceName, String projectName, String issueType,
                               List<String> itemIds) {
        this.state = state.orElse(null);
        this.sourceName = sourceName;
        this.projectName = projectName;
        this.issueType = issueType;
    }

    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return sourceName + "|" + projectName + "|" + issueType;
    }

    @Override
    public Optional<SaasWorkerProgressState> getProgressState() {
        if (state != null) {
            return Optional.of(state);
        }
        return Optional.empty();
    }

}
