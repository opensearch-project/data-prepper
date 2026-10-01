package org.opensearch.dataprepper.plugins.source.s3.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FolderPartitioningOptions {

    @JsonProperty("depth")
    private Integer folderDepth = 1;

    @JsonProperty("max_objects_per_ownership")
    private Integer objectsPerOwnership = 50;

    @JsonProperty("idle_scan_mode")
    private IdleScanMode idleScanMode = IdleScanMode.IDLE_OPTIMIZED;

    public Integer getFolderDepth() {
        return folderDepth;
    }

    public Integer getMaxObjectsPerOwnership() {
        return objectsPerOwnership;
    }

    public IdleScanMode getIdleScanMode() {
        return idleScanMode;
    }
}
