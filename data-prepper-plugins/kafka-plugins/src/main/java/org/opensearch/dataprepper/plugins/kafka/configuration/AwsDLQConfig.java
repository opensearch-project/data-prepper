/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public class AwsDLQConfig {

    @JsonProperty("bucket")
    @NotNull
    @NotEmpty
    private String bucket;

    @JsonProperty("sts_role_arn")
    @NotNull
    @NotEmpty
    @Size(min = 20, max = 2048, message = "awsStsRoleArn length should be between 1 and 2048 characters")
    private String roleArn;

    @JsonProperty("region")
    @Size(min = 1, message = "Region cannot be empty string")
    private String region;

    public String getBucket() {
        return bucket;
    }

    public String getRoleArn() {
        return roleArn;
    }

    public String getRegion() {
        return region;
    }
}
