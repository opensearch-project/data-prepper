/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
/**
 * Class consists the bucket properties.
 */
public class S3ScanBucketOptions {
    @JsonProperty("bucket")
    private S3ScanBucketOption scanBucketOption;

    public S3ScanBucketOption getS3ScanBucketOption() {
        return scanBucketOption;
    }
}
