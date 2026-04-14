/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.jdbc;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;

import java.time.Instant;

public class JdbcPartitionItem implements SourcePartitionStoreItem {

    private String sourceIdentifier;
    private String sourcePartitionKey;
    private String partitionOwner;
    private String partitionProgressState;
    private SourcePartitionStatus sourcePartitionStatus;
    private Instant partitionOwnershipTimeout;
    private Instant reOpenAt;
    private Long closedCount;
    private String partitionPriority;
    private long version;

    @Override
    public String getSourceIdentifier() {
        return sourceIdentifier;
    }

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

    public String getPartitionPriority() {
        return partitionPriority;
    }

    public long getVersion() {
        return version;
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

    public void setSourceIdentifier(final String sourceIdentifier) {
        this.sourceIdentifier = sourceIdentifier;
    }

    public void setPartitionPriority(final String partitionPriority) {
        this.partitionPriority = partitionPriority;
    }

    public void setVersion(final long version) {
        this.version = version;
    }
}
