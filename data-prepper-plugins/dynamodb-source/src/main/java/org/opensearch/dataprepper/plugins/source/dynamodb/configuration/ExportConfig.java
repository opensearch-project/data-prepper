/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.regions.Region;

public class ExportConfig {

    @JsonProperty("s3_bucket")
    @NotBlank(message = "Bucket Name is required for export")
    private String s3Bucket;
    @JsonProperty("s3_prefix")
    private String s3Prefix;

    @JsonProperty("s3_region")
    private String s3Region;

    @JsonProperty("s3_sse_kms_key_id")
    private String s3SseKmsKeyId;

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getS3Prefix() {
        return s3Prefix;
    }

    public Region getAwsRegion() {
        return s3Region != null ? Region.of(s3Region) : null;
    }

    public String getS3SseKmsKeyId() {
        return s3SseKmsKeyId;
    }

    @AssertTrue(message = "KMS Key ID must be a valid one.")
    boolean isKmsKeyIdValid() {
        // If key id is provided, it should be in a format like
        // arn:aws:kms:us-west-2:123456789012:key/0a4bc22f-bb96-4ad3-80ca-63b12b3ec147
        return s3SseKmsKeyId == null || Arn.fromString(s3SseKmsKeyId).resourceAsString() != null;
    }

}
