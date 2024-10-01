package org.opensearch.dataprepper.plugins.source.neptune.stream;

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

    public void setAcknowledgedTimestamp(final Long acknowledgedTimestamp) {
        this.acknowledgedTimestamp = acknowledgedTimestamp;
    }

    public void setAcknowledged(final AcknowledgmentStatus acknowledgmentStatus) {
        this.acknowledgeStatus = acknowledgmentStatus;
    }

    public Long getCommitNum() {
        return commitNum;
    }

    public Long getOpNum() {
        return opNum;
    }

    public Long getRecordCount() {
        return recordCount;
    }

    public boolean isPositiveAcknowledgement() {
        return this.acknowledgeStatus == AcknowledgmentStatus.POSITIVE_ACK;
    }

    public boolean isNegativeAcknowledgement() {
        return this.acknowledgeStatus == AcknowledgmentStatus.NEGATIVE_ACK;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public long getAcknowledgedTimestamp() {
        return acknowledgedTimestamp;
    }

}
