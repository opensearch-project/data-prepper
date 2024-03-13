/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataQueryProgressState {


    @JsonProperty("executedQueries")
    private int executedQueries;

    @JsonProperty("loadedRecords")
    private int loadedRecords;

    @JsonProperty("exportStartTime")
    private long startTime;

    public int getExecutedQueries() {
        return executedQueries;
    }

    public int getLoadedRecords() {
        return loadedRecords;
    }

    public void setExecutedQueries(int executedQueries) {
        this.executedQueries = executedQueries;
    }

    public void setLoadedRecords(int loadedRecords) {
        this.loadedRecords = loadedRecords;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStartTime() {
        return startTime;
    }
}
