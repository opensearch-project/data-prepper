package org.opensearch.dataprepper.plugins.mongo.model;

import java.util.Map;

public class StreamLoadStatus {

    private static final String EXPORT_END_TIMESTAMP = "exportEndTimestamp";

    private long exportEndTimestamp;

    public StreamLoadStatus(long exportEndTimestamp) {
        this.exportEndTimestamp = exportEndTimestamp;
    }

    public void setExportEndTimestamp(long exportEndTimestamp) {
        this.exportEndTimestamp = exportEndTimestamp;
    }

    public long getExportEndTimestamp() {
        return exportEndTimestamp;
    }

    public Map<String, Object> toMap() {
        return Map.of(
                EXPORT_END_TIMESTAMP, exportEndTimestamp
        );
    }

    public static StreamLoadStatus fromMap(Map<String, Object> map) {
        return new StreamLoadStatus(
                ((Number) map.get(EXPORT_END_TIMESTAMP)).longValue()
        );
    }
}
