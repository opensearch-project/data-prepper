/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStatus;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondarySortKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import java.time.Instant;

@DynamoDbBean
public class DynamoDbSourcePartitionItem implements SourcePartitionStoreItem {

    private String sourceIdentifier;
    private String sourcePartitionKey;
    private String partitionOwner;
    private Long version;
    private String partitionProgressState;
    private SourcePartitionStatus sourcePartitionStatus;
    private Instant partitionOwnershipTimeout;
    private Instant reOpenAt;
    private Long closedCount;
    private String sourceStatusCombinationKey;
    private String partitionPriority;
    private Long expirationTime;

    @Override
    @DynamoDbPartitionKey
    public String getSourceIdentifier() {
        return sourceIdentifier;
    }

    @Override
    @DynamoDbSortKey
    public String getSourcePartitionKey() {
        return sourcePartitionKey;
    }

    @DynamoDbSecondaryPartitionKey(indexNames = "source-status")
    public String getSourceStatusCombinationKey() {
        return sourceStatusCombinationKey;
    }

    @DynamoDbSecondarySortKey(indexNames = "source-status")
    public String getPartitionPriority() {
        return partitionPriority;
    }

    public Long getExpirationTime() { return expirationTime; }

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

    public void setSourceIdentifier(final String sourceIdentifier) {
        this.sourceIdentifier = sourceIdentifier;
    }

    public void setSourceStatusCombinationKey(final String sourceStatusCombinationKey) {
        this.sourceStatusCombinationKey = sourceStatusCombinationKey;
    }

    public void setPartitionPriority(final String partitionPriority) {
        this.partitionPriority = partitionPriority;
    }

    public void setExpirationTime(final Long expirationTime) {
        this.expirationTime = expirationTime;
    }
}
