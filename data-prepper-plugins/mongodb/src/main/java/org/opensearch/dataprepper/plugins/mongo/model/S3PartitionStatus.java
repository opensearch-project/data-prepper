package org.opensearch.dataprepper.plugins.mongo.model;

import java.util.Map;

public class S3PartitionStatus {

    private static final String TOTAL_PARTITIONS = "totalPartitions";

    private final int totalPartitions;

    public S3PartitionStatus(int totalPartitions) {
        this.totalPartitions = totalPartitions;
    }

    public int getTotalPartitions() {
        return totalPartitions;
    }

    public Map<String, Object> toMap() {
        return Map.of(
                TOTAL_PARTITIONS, totalPartitions
        );
    }

    public static S3PartitionStatus fromMap(Map<String, Object> map) {
        return new S3PartitionStatus(
                ((Number) map.get(TOTAL_PARTITIONS)).intValue()
        );
    }
}
