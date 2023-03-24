/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.sourcecoordination.dynamo;

import org.opensearch.dataprepper.sourcecoordination.SourcePartitionStatus;
import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.EnumAttributeConverter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbConvertedBy;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import java.time.Instant;

@DynamoDbBean
public class DynamoDbSourcePartitionItem {

    private String sourcePartitionKey;
    private String partitionOwner;
    private Long version;
    private Object partitionProgressState;
    private SourcePartitionStatus sourcePartitionStatus;
    private Instant partitionOwnershipTimeout;
    private Instant reOpenAt;
    private Long closedCount;

    public void setSourcePartitionKey(final String sourcePartitionKey) {
        this.sourcePartitionKey = sourcePartitionKey;
    }

    public void setPartitionOwner(final String partitionOwner) {
        this.partitionOwner = partitionOwner;
    }

    public void setVersion(final Long version) {
        this.version = version;
    }

    public void setPartitionProgressState(final Object partitionProgressState) {
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

    @DynamoDbPartitionKey
    private String getSourcePartitionKey() {
        return this.sourcePartitionKey;
    }

    @DynamoDbSortKey
    public String getPartitionOwner() {
        return partitionOwner;
    }

    public Long getVersion() {
        return version;
    }

    public Object getPartitionProgressState() {
        return partitionProgressState;
    }

    // Global secondary index? Could be configurable
    @DynamoDbSecondaryPartitionKey(indexNames = {"sourcePartitionStatus"})
    @DynamoDbConvertedBy(EnumAttributeConverter.class)
    public SourcePartitionStatus getSourcePartitionStatus() {
        return sourcePartitionStatus;
    }

    public Instant getPartitionOwnershipTimeout() {
        return partitionOwnershipTimeout;
    }

    public Instant getReOpenAt() {
        return reOpenAt;
    }

    public Long getClosedCount() {
        return closedCount;
    }

    @Override
    public String toString() {
        return "DynamoDbSourcePartitionItem{" +
                "sourcePartitionKey='" + sourcePartitionKey + '\'' +
                ", partitionOwner='" + partitionOwner + '\'' +
                ", version=" + version +
                ", partitionProgressState=" + partitionProgressState +
                ", sourcePartitionStatus=" + sourcePartitionStatus +
                ", partitionOwnershipTimeout=" + partitionOwnershipTimeout +
                ", reOpenAt=" + reOpenAt +
                ", closedCount=" + closedCount +
                '}';
    }
}
