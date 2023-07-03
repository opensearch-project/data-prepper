/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;

public class AwsConfig {
    @JsonProperty("msk_arn")
    @Size(min = 20, max = 2048, message = "mskArn length should be between 20 and 2048 characters")
    private String awsMskArn;

    public String getAwsMskArn() {
        return awsMskArn;
    }
}
