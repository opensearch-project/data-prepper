/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.xrayotlp.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import software.amazon.awssdk.regions.Region;

import static org.opensearch.dataprepper.plugins.sink.xrayotlp.XRayOTLPSinkConfig.DEFAULT_AWS_REGION;

/**
 * Configuration class for AWS authentication settings.
 * Handles region, STS role ARN, and external ID configurations required for AWS service access.
 *
 * @since 2.6
 */
public class AwsAuthenticationConfiguration {
    /**
     * AWS region for X-Ray service.
     * Must be a valid AWS region identifier (e.g., us-east-1, us-west-2).
     */
    @JsonProperty("region")
    @Size(min = 1, message = "Region cannot be empty string")
    public String awsRegion = DEFAULT_AWS_REGION;

    /**
     * AWS STS Role ARN for assuming role-based access.
     * Format: arn:aws:iam::{account}:role/{role-name}
     * Length must be between 20 and 2048 characters.
     */
    @Getter
    @JsonProperty("sts_role_arn")
    @Size(min = 20, max = 2048, message = "awsStsRoleArn length should be between 1 and 2048 characters")
    public String awsStsRoleArn;

    /**
     * External ID for additional security when assuming an IAM role.
     * Required only if the trust policy requires an external ID.
     * Length must be between 2 and 1224 characters.
     */
    @Getter
    @JsonProperty("sts_external_id")
    @Size(min = 2, max = 1224, message = "awsStsExternalId length should be between 2 and 1224 characters")
    public String awsStsExternalId;

    /**
     * Gets the AWS Region object corresponding to the configured region string.
     *
     * @return Region object if awsRegion is set, null otherwise
     */
    public Region getAwsRegion() {
        return awsRegion != null ? Region.of(awsRegion) : null;
    }
}
