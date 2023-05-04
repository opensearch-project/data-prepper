/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.inmemory;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import java.time.Instant;

public class InMemorySourcePartitionStoreItem implements SourcePartitionStoreItem {
    private String sourcePartitionKey;
    private String partitionOwner;
    private String partitionProgressState;
    private SourcePartitionStatus sourcePartitionStatus;
    private Instant partitionOwnershipTimeout;
    private Instant reOpenAt;
    private Long closedCount;

    @Override
    public String getSourcePartitionKey() {
        return sourcePartitionKey;
    }

    @Override
    public String getPartitionOwner() {
        return partitionOwner;
    }

    @Override
    public String getPartitionProgressState() {
        return partitionProgressState;
    }

    @Override
    public SourcePartitionStatus getSourcePartitionStatus() {
        return sourcePartitionStatus;
    }

    @Override
    public Instant getPartitionOwnershipTimeout() {
        return partitionOwnershipTimeout;
    }

    @Override
    public Instant getReOpenAt() {
        return reOpenAt;
    }

    @Override
    public Long getClosedCount() {
        return closedCount;
    }

    @Override
    public void setSourcePartitionKey(final String sourcePartitionKey) {
        this.sourcePartitionKey = sourcePartitionKey;
    }

    @Override
    public void setPartitionOwner(final String partitionOwner) {
        this.partitionOwner = partitionOwner;
    }

    @Override
    public void setPartitionProgressState(final String partitionProgressState) {
        this.partitionProgressState = partitionProgressState;
    }

    @Override
    public void setSourcePartitionStatus(final SourcePartitionStatus sourcePartitionStatus) {
        this.sourcePartitionStatus = sourcePartitionStatus;
    }

    @Override
    public void setPartitionOwnershipTimeout(final Instant partitionOwnershipTimeout) {
        this.partitionOwnershipTimeout = partitionOwnershipTimeout;
    }

    @Override
    public void setReOpenAt(final Instant reOpenAt) {
        this.reOpenAt = reOpenAt;
    }

    @Override
    public void setClosedCount(final Long closedCount) {
        this.closedCount = closedCount;
    }
}
