/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

import java.time.Instant;

@DynamoDbBean
public class DynamoDbSourcePartitionItem implements SourcePartitionStoreItem {

    private String sourcePartitionKey;
    private String partitionOwner;
    private Long version;
    private String partitionProgressState;
    private SourcePartitionStatus sourcePartitionStatus;
    private Instant partitionOwnershipTimeout;
    private Instant reOpenAt;
    private Long closedCount;

    @Override
    @DynamoDbPartitionKey
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

    public Long getVersion() {
        return version;
    }

    public void setSourcePartitionKey(final String sourcePartitionKey) {
        this.sourcePartitionKey = sourcePartitionKey;
    }

    public void setPartitionOwner(final String partitionOwner) {
        this.partitionOwner = partitionOwner;
    }

    public void setVersion(final Long version) {
        this.version = version;
    }

    public void setPartitionProgressState(final String partitionProgressState) {
        this.partitionProgressState = partitionProgressState;
    }

    // TODO: Make this a global secondary index to improve performance to scan for available partitions only on unassigned or closed partitions (can be optional setting on the store)
    public void setSourcePartitionStatus(final SourcePartitionStatus sourcePartitionStatus) {
        this.sourcePartitionStatus = sourcePartitionStatus;
    }

    public void setPartitionOwnershipTimeout(final Instant partitionOwnershipTimeout) {
        this.partitionOwnershipTimeout = partitionOwnershipTimeout;
    }

    public void setReOpenAt(final Instant reOpenAt) {
        this.reOpenAt = reOpenAt;
    }

    public void setClosedCount(final Long closedCount) {
        this.closedCount = closedCount;
    }


}
