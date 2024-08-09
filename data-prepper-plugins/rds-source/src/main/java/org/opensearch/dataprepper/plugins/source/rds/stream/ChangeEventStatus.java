/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;

public class ChangeEventStatus {

    private final BinlogCoordinate binlogCoordinate;
    private final long timestamp;
    private volatile AcknowledgmentStatus acknowledgmentStatus;

    public enum AcknowledgmentStatus {
        POSITIVE_ACK,
        NEGATIVE_ACK,
        NO_ACK
    }

    public ChangeEventStatus(final BinlogCoordinate binlogCoordinate, final long timestamp) {
        this.binlogCoordinate = binlogCoordinate;
        this.timestamp = timestamp;
        acknowledgmentStatus = AcknowledgmentStatus.NO_ACK;
    }

    public void setAcknowledgmentStatus(final AcknowledgmentStatus acknowledgmentStatus) {
        this.acknowledgmentStatus = acknowledgmentStatus;
    }

    public AcknowledgmentStatus getAcknowledgmentStatus() {
        return acknowledgmentStatus;
    }

    public boolean isPositiveAcknowledgment() {
        return acknowledgmentStatus == AcknowledgmentStatus.POSITIVE_ACK;
    }

    public boolean isNegativeAcknowledgment() {
        return acknowledgmentStatus == AcknowledgmentStatus.NEGATIVE_ACK;
    }

    public BinlogCoordinate getBinlogCoordinate() {
        return binlogCoordinate;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
