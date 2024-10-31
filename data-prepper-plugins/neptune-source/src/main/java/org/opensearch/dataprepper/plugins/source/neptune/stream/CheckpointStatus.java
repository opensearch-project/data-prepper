package org.opensearch.dataprepper.plugins.source.neptune.stream;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CheckpointStatus {
    private final Long commitNum;
    private final Long opNum;
    private final Long recordCount;
    private AcknowledgmentStatus acknowledgeStatus;
    private final long createTimestamp;
    private Long acknowledgedTimestamp;

    enum AcknowledgmentStatus {
        POSITIVE_ACK,
        NEGATIVE_ACK,
        NO_ACK
    }

    public CheckpointStatus(final Long commitNum, final Long opNum, final Long recordCount, final long createTimestamp) {
        this.commitNum = commitNum;
        this.opNum = opNum;
        this.recordCount = recordCount;
        this.acknowledgeStatus = AcknowledgmentStatus.NO_ACK;
        this.createTimestamp = createTimestamp;
    }

    public boolean isPositiveAcknowledgement() {
        return this.acknowledgeStatus == AcknowledgmentStatus.POSITIVE_ACK;
    }

    public boolean isNegativeAcknowledgement() {
        return this.acknowledgeStatus == AcknowledgmentStatus.NEGATIVE_ACK;
    }
}
