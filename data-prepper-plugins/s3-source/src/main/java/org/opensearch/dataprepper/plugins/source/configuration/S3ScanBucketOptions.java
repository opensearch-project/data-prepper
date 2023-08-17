/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;

/**
 * Class consists the bucket properties.
 */
public class S3ScanBucketOptions {
    @JsonProperty("bucket")
    @Valid
    private S3ScanBucketOption scanBucketOption;

    public S3ScanBucketOption getS3ScanBucketOption() {
        return scanBucketOption;
    }
}
