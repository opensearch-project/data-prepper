/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class consists the bucket related configuration properties.
 */
public class S3ScanBucketOption {
    @JsonProperty("name")
    private String name;
    @JsonProperty("key_path")
    private S3ScanKeyPathOption keyPath;
    @JsonProperty("compression")
    private CompressionOption compression;

    public String getName() {
        return name;
    }

    public S3ScanKeyPathOption getKeyPath() {
        return keyPath;
    }

    public CompressionOption getCompression() {
        return compression;
    }
}