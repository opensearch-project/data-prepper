package org.opensearch.dataprepper.plugins.mongo.stream;

public class CheckpointStatus {
    private final String resumeToken;
    private final long recordCount;
    private boolean acknowledged;
    private final long createTimestamp;
    private Long acknowledgedTimestamp;

    public CheckpointStatus(final String resumeToken, final long recordCount, final long createTimestamp) {
        this.resumeToken = resumeToken;
        this.recordCount = recordCount;
        this.acknowledged = false;
        this.createTimestamp = createTimestamp;
    }

    public void setAcknowledgedTimestamp(final Long acknowledgedTimestamp) {
        this.acknowledgedTimestamp = acknowledgedTimestamp;
    }

    public void setAcknowledged(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public String getResumeToken() {
        return resumeToken;
    }
    public long getRecordCount() {
        return recordCount;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public long getAcknowledgedTimestamp() {
        return acknowledgedTimestamp;
    }


}
