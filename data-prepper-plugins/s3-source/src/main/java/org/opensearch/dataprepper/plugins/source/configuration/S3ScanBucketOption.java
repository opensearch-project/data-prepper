/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.util.List;

/**
 * Class consists the bucket related configuration properties.
 */
public class S3ScanBucketOption {
    @JsonProperty("name")
    private String name;
    @JsonProperty("codec")
    private PluginModel codec;
    @JsonProperty("s3_select")
    private S3SelectOptions s3SelectOptions;
    @JsonProperty("key_path")
    private List<String> keyPath;
    @JsonProperty("compression")
    private CompressionOption compression;

    public String getName() {
        return name;
    }

    public PluginModel getCodec() {
        return codec;
    }

    public S3SelectOptions getS3SelectOptions() {
        return s3SelectOptions;
    }

    public List<String> getKeyPath() {
        return keyPath;
    }

    public CompressionOption getCompression() {
        return compression;
    }
}