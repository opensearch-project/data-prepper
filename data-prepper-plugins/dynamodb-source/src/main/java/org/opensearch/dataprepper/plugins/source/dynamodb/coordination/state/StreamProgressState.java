/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamProgressState {

    @JsonProperty("startTime")
    private long startTime;

    @JsonProperty("sequenceNumber")
    private String sequenceNumber;


    @JsonProperty("waitForExport")
    private boolean waitForExport = false;

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(String sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public boolean shouldWaitForExport() {
        return waitForExport;
    }

    public void setWaitForExport(boolean waitForExport) {
        this.waitForExport = waitForExport;
    }
}
