package org.opensearch.dataprepper.plugins.mongo.stream;

public class CheckpointStatus {
    private final String resumeToken;
    private final long recordCount;
    private AcknowledgmentStatus acknowledgeStatus;
    private final long createTimestamp;
    private Long acknowledgedTimestamp;

    enum AcknowledgmentStatus {
        POSITIVE_ACK,
        NEGATIVE_ACK,
        NO_ACK
    }

    public CheckpointStatus(final String resumeToken, final long recordCount, final long createTimestamp) {
        this.resumeToken = resumeToken;
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

    public String getResumeToken() {
        return resumeToken;
    }
    public long getRecordCount() {
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
