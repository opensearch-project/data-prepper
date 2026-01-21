/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import jakarta.validation.Valid;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;

import java.util.Map;

public class AwsConfig implements AwsCredentialsConfig {

    public static class AwsMskConfig {
        @Valid
        @Size(min = 20, max = 2048, message = "msk_arn length should be between 20 and 2048 characters")
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

    @Valid
    @Size(min = 1, message = "Region cannot be empty string")
    @JsonProperty("region")
    private String region;

    @Valid
    @Size(min = 20, max = 2048, message = "sts_role_arn length should be between 20 and 2048 characters")
    @JsonProperty("sts_role_arn")
    private String stsRoleArn;

    @JsonProperty("sts_header_overrides")
    @Size(max = 5, message = "sts_header_overrides supports a maximum of 5 headers to override")
    private Map<String, String> awsStsHeaderOverrides;

    @JsonProperty("role_session_name")
    private String stsRoleSessionName;

    public AwsMskConfig getAwsMskConfig() {
        return awsMskConfig;
    }

    @Override
    public String getRegion() {
        return region;
    }

    @Override
    public String getStsRoleArn() {
        return stsRoleArn;
    }

    public String getStsRoleSessionName() {
        return stsRoleSessionName;
    }

    public Map<String, String> getAwsStsHeaderOverrides() {
        return awsStsHeaderOverrides;
    }

    @Override
    public AwsCredentialsOptions toCredentialsOptions() {
        return AwsCredentialsOptions.builder()
                .withRegion(region)
                .withStsRoleArn(stsRoleArn)
                .withStsHeaderOverrides(awsStsHeaderOverrides)
                .build();
    }
}
