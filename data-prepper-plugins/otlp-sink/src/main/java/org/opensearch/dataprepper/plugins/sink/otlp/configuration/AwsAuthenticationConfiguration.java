/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.regions.Region;

/**
 * Configuration class for AWS authentication settings.
 * Handles region, STS role ARN, and external ID configurations required for AWS service access.
 * This class will be automatically wired by Data-Prepper.
 *
 * @since 2.6
 */
@Getter
@NoArgsConstructor
class AwsAuthenticationConfiguration {
    /**
     * AWS region.
     * Must be a valid AWS region identifier (e.g., us-east-1, us-west-2).
     */
    @JsonProperty("region")
    @Size(min = 1, message = "Region cannot be empty string")
    private String awsRegion;

    /**
     * AWS STS Role ARN for assuming role-based access.
     * Format: arn:aws:iam::{account}:role/{role-name}
     * Length must be between 20 and 2048 characters.
     */
    @JsonProperty("sts_role_arn")
    @Size(min = 20, max = 2048, message = "awsStsRoleArn length should be between 1 and 2048 characters")
    private String awsStsRoleArn;

    /**
     * External ID for additional security when assuming an IAM role.
     * Required only if the trust policy requires an external ID.
     * Length must be between 2 and 1224 characters.
     */
    @JsonProperty("sts_external_id")
    @Size(min = 2, max = 1224, message = "awsStsExternalId length should be between 2 and 1224 characters")
    private String awsStsExternalId;

    /**
     * Gets the AWS Region object corresponding to the configured region string.
     *
     * @return Region object if awsRegion is set, otherwise returns null.
     * Note: Default region fallback is handled externally by the caller.
     */
    Region getAwsRegion() {
        return awsRegion != null ? Region.of(awsRegion) : null;
    }
}
