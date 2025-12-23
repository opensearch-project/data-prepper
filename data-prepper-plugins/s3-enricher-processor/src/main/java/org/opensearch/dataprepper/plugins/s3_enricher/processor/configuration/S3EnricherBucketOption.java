/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.s3_enricher.processor.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.Getter;

/**
 * Class consists the bucket related configuration properties.
 */
@Getter
public class S3EnricherBucketOption {

    @JsonProperty("name")
    @NotEmpty
    @Size(min = 3, max = 500, message = "bucket length should be at least 3 characters")
    private String name;

    @JsonProperty("filter")
    private S3EnricherKeyPathOption s3SourceFilter;

    public S3EnricherKeyPathOption getS3SourceFilter() {
        return s3SourceFilter;
    }
}
