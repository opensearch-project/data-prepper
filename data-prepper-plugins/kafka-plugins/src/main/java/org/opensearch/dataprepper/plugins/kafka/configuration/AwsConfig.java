/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import jakarta.validation.Valid;

public class AwsConfig {
    @Valid
    @Size(min = 20, max = 2048, message = "msk_arn length should be between 20 and 2048 characters")
    @JsonProperty("msk_arn")
    private String mskArn;

    @Valid
    @Size(min = 1, message = "Region cannot be empty string")
    @JsonProperty("region")
    private String region;

    @Valid
    @Size(min = 20, max = 2048, message = "sts_role_arn length should be between 20 and 2048 characters")
    @JsonProperty("sts_role_arn")
    private String stsRoleArn;

    public String getMskArn() {
        return mskArn;
    }

    public String getRegion() {
        return region;
    }

    public String getStsRoleArn() {
        return stsRoleArn;
    }
}
