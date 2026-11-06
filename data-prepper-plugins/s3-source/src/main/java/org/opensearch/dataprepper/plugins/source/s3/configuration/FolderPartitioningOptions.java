package org.opensearch.dataprepper.plugins.source.s3.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;

import java.time.Duration;

public class FolderPartitioningOptions {

    @JsonProperty("depth")
    private Integer folderDepth = 1;

    @JsonProperty("max_objects_per_ownership")
    private Integer objectsPerOwnership = 50;

    @JsonProperty("idle_scan_mode")
    private IdleScanMode idleScanMode;

    /**
     * Minimum time to wait before a folder partition that returned no objects is listed again.
     * While waiting, the folder partition stays leased so that no worker re-lists it, which bounds
     * the number of S3 ListObjectsV2 calls made against quiet folders. Defaults to zero (no delay).
     */
    @JsonProperty("empty_folder_rescan_delay")
    @DurationMin(seconds = 0)
    @DurationMax(minutes = 30)
    private Duration emptyFolderRescanDelay = Duration.ZERO;

    public Integer getFolderDepth() {
        return folderDepth;
    }

    public Integer getMaxObjectsPerOwnership() {
        return objectsPerOwnership;
    }

    public IdleScanMode getIdleScanMode() {
        return idleScanMode != null ? idleScanMode : IdleScanMode.IDLE_OPTIMIZED;
    }

    public boolean isContinuousScanMode() {
        return getIdleScanMode() == IdleScanMode.CONTINUOUS;
    }

    public Duration getEmptyFolderRescanDelay() {
        return emptyFolderRescanDelay != null ? emptyFolderRescanDelay : Duration.ZERO;
    }
}
