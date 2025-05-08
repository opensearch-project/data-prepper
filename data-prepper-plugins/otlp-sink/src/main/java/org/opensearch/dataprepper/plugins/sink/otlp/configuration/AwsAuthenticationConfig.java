/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Configuration class for AWS authentication settings.
 * This class will be automatically wired by Data-Prepper.
 */
@Getter
@NoArgsConstructor
class AwsAuthenticationConfig {

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
}
