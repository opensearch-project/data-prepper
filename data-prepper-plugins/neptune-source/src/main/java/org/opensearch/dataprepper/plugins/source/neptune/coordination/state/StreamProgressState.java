package org.opensearch.dataprepper.plugins.source.neptune.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamProgressState {

    @JsonProperty("startTime")
    private long startTime;

    @JsonProperty("commitNum")
    private Long commitNum;

    @JsonProperty("opNum")
    private Long opNum;

    @JsonProperty("loadedRecords")
    private Long loadedRecords;

    @JsonProperty("lastUpdateTimestamp")
    private long lastUpdateTimestamp;

    @JsonProperty("waitForExport")
    private boolean waitForExport = false;

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public Long getCommitNum() {
        return this.commitNum;
    }

    public void setCommitNum(long commitNum) {
        this.commitNum = commitNum;
    }

    public Long getOpNum() {
        return this.opNum;
    }

    public void setOpNum(long opNum) {
        this.opNum = opNum;
    }

    public Long getLoadedRecords() {
        return loadedRecords;
    }

    public void setLoadedRecords(long loadedRecords) {
        this.loadedRecords = loadedRecords;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }
}
