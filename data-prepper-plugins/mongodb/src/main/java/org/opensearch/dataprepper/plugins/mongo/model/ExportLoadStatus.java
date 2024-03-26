package org.opensearch.dataprepper.plugins.mongo.model;

import java.util.Map;

public class ExportLoadStatus {

    private static final String TOTAL_PARTITIONS = "totalPartitions";
    private static final String LOADED_PARTITIONS = "loadedPartitions";
    private static final String LOADED_RECORDS = "loadedRecords";

    private static final String LAST_UPDATE_TIMESTAMP = "lastUpdateTimestamp";

    private long totalPartitions;
    private long loadedPartitions;
    private long loadedRecords;
    private long lastUpdateTimestamp;

    public ExportLoadStatus(long totalPartitions, long loadedPartitions, long loadedRecords, long lastUpdateTimestamp) {
        this.totalPartitions = totalPartitions;
        this.loadedPartitions = loadedPartitions;
        this.loadedRecords = loadedRecords;
        this.lastUpdateTimestamp = lastUpdateTimestamp;
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

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public Map<String, Object> toMap() {
        return Map.of(
                TOTAL_PARTITIONS, totalPartitions,
                LOADED_PARTITIONS, loadedPartitions,
                LOADED_RECORDS, loadedRecords,
                LAST_UPDATE_TIMESTAMP, lastUpdateTimestamp
        );
    }

    public static ExportLoadStatus fromMap(Map<String, Object> map) {
        return new ExportLoadStatus(
                ((Number) map.get(TOTAL_PARTITIONS)).intValue(),
                ((Number) map.get(LOADED_PARTITIONS)).intValue(),
                ((Number) map.get(LOADED_RECORDS)).longValue(),
                ((Number) map.get(LAST_UPDATE_TIMESTAMP)).longValue()
        );
    }
}
