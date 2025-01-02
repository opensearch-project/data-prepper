/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.neptune.model;

import java.util.Map;

public class ExportLoadStatus {

    public static final String TOTAL_PARTITIONS = "totalPartitions";
    public static final String LOADED_PARTITIONS = "loadedPartitions";
    public static final String LOADED_RECORDS = "loadedRecords";

    public static final String LAST_UPDATE_TIMESTAMP = "lastUpdateTimestamp";
    public static final String TOTAL_PARTITIONS_COMPLETE = "totalPartitionsComplete";

    private long totalPartitions;
    private long loadedPartitions;
    private long loadedRecords;
    private long lastUpdateTimestamp;
    private boolean isTotalParitionsComplete;

    public ExportLoadStatus(long totalPartitions,
                            long loadedPartitions,
                            long loadedRecords,
                            long lastUpdateTimestamp,
                            boolean isTotalParitionsComplete) {
        this.totalPartitions = totalPartitions;
        this.loadedPartitions = loadedPartitions;
        this.loadedRecords = loadedRecords;
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.isTotalParitionsComplete = isTotalParitionsComplete;
    }

    public long getTotalPartitions() {
        return totalPartitions;
    }

    public void setLoadedPartitions(long loadedPartitions) {
        this.loadedPartitions = loadedPartitions;
    }

    public long getLoadedPartitions() {
        return loadedPartitions;
    }

    public void setTotalPartitions(long totalPartitions) {
        this.totalPartitions = totalPartitions;
    }

    public long getLoadedRecords() {
        return loadedRecords;
    }

    public void setLoadedRecords(long loadedRecords) {
        this.loadedRecords = loadedRecords;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public boolean isTotalParitionsComplete() {
        return isTotalParitionsComplete;
    }

    public void setTotalParitionsComplete(boolean totalParitionsComplete) {
        isTotalParitionsComplete = totalParitionsComplete;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public Map<String, Object> toMap() {
        return Map.of(
                TOTAL_PARTITIONS, totalPartitions,
                LOADED_PARTITIONS, loadedPartitions,
                LOADED_RECORDS, loadedRecords,
                LAST_UPDATE_TIMESTAMP, lastUpdateTimestamp,
                TOTAL_PARTITIONS_COMPLETE, isTotalParitionsComplete
        );
    }

    public static ExportLoadStatus fromMap(Map<String, Object> map) {
        return new ExportLoadStatus(
                ((Number) map.get(TOTAL_PARTITIONS)).intValue(),
                ((Number) map.get(LOADED_PARTITIONS)).intValue(),
                ((Number) map.get(LOADED_RECORDS)).longValue(),
                ((Number) map.get(LAST_UPDATE_TIMESTAMP)).longValue(),
                (Boolean) map.get(TOTAL_PARTITIONS_COMPLETE)
        );
    }
}
