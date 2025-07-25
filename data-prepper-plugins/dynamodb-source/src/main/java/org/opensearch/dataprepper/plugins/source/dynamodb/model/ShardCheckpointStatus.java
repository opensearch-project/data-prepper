/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.model;

import java.time.Duration;
import java.time.Instant;

public class ShardCheckpointStatus {
    private final String sequenceNumber;

    private final boolean isFinalAcknowledgmentForPartition;
    private AcknowledgmentStatus acknowledgeStatus;
    private final long createTimestamp;
    private Long acknowledgedTimestamp;

    public enum AcknowledgmentStatus {
        POSITIVE_ACK,
        NEGATIVE_ACK,
        NO_ACK
    }

    public ShardCheckpointStatus(final String sequenceNumber, final long createTimestamp, final boolean isFinalAcknowledgmentForPartition) {
        this.sequenceNumber = sequenceNumber;
        this.acknowledgeStatus = AcknowledgmentStatus.NO_ACK;
        this.createTimestamp = createTimestamp;
        this.isFinalAcknowledgmentForPartition = isFinalAcknowledgmentForPartition;
    }

    public void setAcknowledgedTimestamp(final Long acknowledgedTimestamp) {
        this.acknowledgedTimestamp = acknowledgedTimestamp;
    }

    public void setAcknowledged(final AcknowledgmentStatus acknowledgmentStatus) {
        this.acknowledgeStatus = acknowledgmentStatus;
    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    public boolean isPositiveAcknowledgement() {
        return this.acknowledgeStatus == AcknowledgmentStatus.POSITIVE_ACK;
    }

    public boolean isNegativeAcknowledgement() {
        return this.acknowledgeStatus == AcknowledgmentStatus.NEGATIVE_ACK;
    }

    public boolean isFinalAcknowledgmentForPartition() {
        return isFinalAcknowledgmentForPartition;
    }

    public boolean isExpired(final Duration expiredDuration) {
        return Duration.between(Instant.ofEpochMilli(createTimestamp), Instant.now()).compareTo(expiredDuration) > 0;
    }

}