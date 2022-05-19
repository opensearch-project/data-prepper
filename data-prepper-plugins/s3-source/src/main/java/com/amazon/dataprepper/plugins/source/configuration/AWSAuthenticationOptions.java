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
    private String region;

    @JsonProperty("aws_sts_role_arn")
    private String stsRoleArn;

    @JsonProperty("access_key_id")
    private String accessKeyId;

    @JsonProperty("secret_key_id")
    private String secretKeyId;

    public String getRegion() {
        return region;
    }

    public String getStsRoleArn() {
        return stsRoleArn;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretKeyId() {
        return secretKeyId;
    }
}
