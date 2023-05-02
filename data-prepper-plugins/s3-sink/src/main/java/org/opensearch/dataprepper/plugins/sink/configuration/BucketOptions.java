/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

/**
 * An implementation class of bucket name and {@link ObjectKeyOptions} configuration Options
 */
public class BucketOptions {

    @JsonProperty("name")
    @NotNull
    private String bucketName;

    @JsonProperty("object_key")
    private ObjectKeyOptions objectKeyOptions;

    /**
     * Read s3 bucket name configuration.
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * S3 {@link ObjectKeyOptions} configuration Options.
     */
    public ObjectKeyOptions getObjectKeyOptions() {
        if (objectKeyOptions == null) {
            objectKeyOptions = new ObjectKeyOptions();
        }
        return objectKeyOptions;
    }
}