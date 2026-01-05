/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.s3_enrich.processor.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.Getter;

/**
 * Class consists the bucket related configuration properties.
 */
@Getter
public class S3EnrichBucketOption {

    @JsonProperty("name")
    @NotEmpty
    @Size(min = 3, max = 500, message = "bucket length should be at least 3 characters")
    private String name;

    @JsonProperty("filter")
    private S3EnrichKeyPathOption s3SourceFilter;

    public S3EnrichKeyPathOption getS3SourceFilter() {
        return s3SourceFilter;
    }
}
