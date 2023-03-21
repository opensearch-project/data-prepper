/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
/**
 * Class consists the scan options list bucket configuration properties.
 */
public class S3ScanScanOptions {
    @JsonProperty("range")
    private String range;
    @JsonProperty("start_time")
    private String startTime;

    @JsonProperty("buckets")
    private List<S3ScanBucketOptions> buckets;

    public String getRange() {
        return range;
    }

    public String getStartTime() {
        return startTime;
    }

    public List<S3ScanBucketOptions> getBuckets() {
        return buckets;
    }
}
