/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DataQueryProgressState {


    @JsonProperty("executedQueries")
    private long executedQueries;

    @JsonProperty("loadedRecords")
    private long loadedRecords;

    @JsonProperty("exportStartTime")
    private long startTime;

    public long getExecutedQueries() {
        return executedQueries;
    }

    public long getLoadedRecords() {
        return loadedRecords;
    }

    public void setExecutedQueries(long executedQueries) {
        this.executedQueries = executedQueries;
    }

    public void setLoadedRecords(long loadedRecords) {
        this.loadedRecords = loadedRecords;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStartTime() {
        return startTime;
    }
}
