/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.plugin.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AwsConfig {

    public static class AwsMskConfig {
        @JsonProperty("arn")
        private String arn;
    
        @JsonProperty("broker_connection_type")
        private MskBrokerConnectionType brokerConnectionType = MskBrokerConnectionType.SINGLE_VPC;

        public String getArn() {
            return arn;
        }

        public MskBrokerConnectionType getBrokerConnectionType() {
            return brokerConnectionType;
        }
    }
        
    @JsonProperty("msk")
    private AwsMskConfig awsMskConfig;

    @JsonProperty("region")
    private String region;

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
