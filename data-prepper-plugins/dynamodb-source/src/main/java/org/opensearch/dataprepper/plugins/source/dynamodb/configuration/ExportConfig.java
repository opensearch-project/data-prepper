/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public class ExportConfig {

    @JsonProperty("s3_bucket")
    @NotBlank(message = "Bucket Name is required for export")
    private String s3Bucket;
    @JsonProperty("s3_prefix")
    private String s3Prefix;

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getS3Prefix() {
        return s3Prefix;
    }
}
