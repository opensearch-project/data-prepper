/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import jakarta.validation.Valid;

public class AwsConfig {

    public static class AwsMskConfig {
        @Valid
        @Size(min = 20, max = 2048, message = "msk_arn length should be between 20 and 2048 characters")
        @JsonProperty("arn")
        private String arn;
    
        @JsonProperty("broker_connection_type")
        private MskBrokerConnectionType brokerConnectionType;

        public String getArn() {
            return arn;
        }

        public MskBrokerConnectionType getBrokerConnectionType() {
            return brokerConnectionType;
        }
    }
        
    @JsonProperty("msk")
    private AwsMskConfig awsMskConfig;

    @Valid
    @Size(min = 1, message = "Region cannot be empty string")
    @JsonProperty("region")
    private String region;

    @Valid
    @Size(min = 20, max = 2048, message = "sts_role_arn length should be between 20 and 2048 characters")
    @JsonProperty("sts_role_arn")
    private String stsRoleArn;

    public AwsMskConfig getAwsMskConfig() {
        return awsMskConfig;
    }

    public String getRegion() {
        return region;
    }

    public String getStsRoleArn() {
        return stsRoleArn;
    }
}
