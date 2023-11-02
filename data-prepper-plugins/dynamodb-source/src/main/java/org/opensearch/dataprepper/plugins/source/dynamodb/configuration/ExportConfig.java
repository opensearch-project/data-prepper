/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import software.amazon.awssdk.regions.Region;

public class ExportConfig {

    @JsonProperty("s3_bucket")
    @NotBlank(message = "Bucket Name is required for export")
    private String s3Bucket;

    @JsonProperty("s3_prefix")
    private String s3Prefix;

    @JsonProperty("s3_region")
    private String s3Region;

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getS3Prefix() {
        return s3Prefix;
    }

    public Region getAwsRegion() {
        return s3Region != null ? Region.of(s3Region) : null;
    }

}
