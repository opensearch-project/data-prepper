package org.opensearch.dataprepper.plugins.mongo.model;

import java.util.Map;

public class LoadStatus {

    static final String TOTAL_PARTITIONS = "totalPartitions";
    private static final String LOADED_PARTITIONS = "loadedPartitions";
    private static final String LOADED_RECORDS = "loadedRecords";

    private long totalPartitions;
    private long loadedPartitions;
    private long loadedRecords;

    public LoadStatus(int totalPartitions, long loadedRecords) {
        this.totalPartitions = totalPartitions;
        this.loadedRecords = loadedRecords;
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

    public Map<String, Object> toMap() {
        return Map.of(
                TOTAL_PARTITIONS, totalPartitions,
                LOADED_PARTITIONS, loadedPartitions,
                LOADED_RECORDS, loadedRecords
        );
    }

    public static LoadStatus fromMap(Map<String, Object> map) {
        return new LoadStatus(
                ((Number) map.get(TOTAL_PARTITIONS)).intValue(),
                ((Number) map.get(LOADED_RECORDS)).longValue()
        );
    }
}
