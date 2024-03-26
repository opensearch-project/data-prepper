/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamProgressState {

    @JsonProperty("startTime")
    private long startTime;

    @JsonProperty("resumeToken")
    private String resumeToken;

    @JsonProperty("loadedRecords")
    private long loadedRecords;

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

    public String getResumeToken() {
        return resumeToken;
    }

    public void setResumeToken(String resumeToken) {
        this.resumeToken = resumeToken;
    }

    public long getLoadedRecords() {
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

    public boolean shouldWaitForExport() {
        return waitForExport;
    }

    public void setWaitForExport(boolean waitForExport) {
        this.waitForExport = waitForExport;
    }
}
