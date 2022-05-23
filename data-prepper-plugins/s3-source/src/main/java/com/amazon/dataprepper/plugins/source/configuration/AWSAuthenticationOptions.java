/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public class AWSAuthenticationOptions {
    @JsonProperty("aws_region")
    @NotBlank(message = "Region cannot be null or empty")
    private String awsRegion;

    @JsonProperty("aws_sts_role_arn")
    private String awsStsRoleArn;

    public String getAwsRegion() {
        return awsRegion;
    }

    public String getAwsStsRoleArn() {
        return awsStsRoleArn;
    }
}

