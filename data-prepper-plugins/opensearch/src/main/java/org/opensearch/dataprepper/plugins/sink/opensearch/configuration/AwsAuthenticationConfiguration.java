/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ServerlessOptions;
import software.amazon.awssdk.regions.Region;

import java.util.Map;

public class AwsAuthenticationConfiguration {

    @JsonProperty("region")
    private String awsRegion;

    @JsonProperty("sts_role_arn")
    @Size(max = 2048, message = "awsStsRoleArn length should be less than 2048 characters")
    private String awsStsRoleArn;

    @JsonProperty("sts_external_id")
    @Size(max = 1224, message = "awsStsExternalId length should be between less than 1224 characters")
    private String awsStsExternalId;

    @JsonProperty("sts_header_overrides")
    @Size(max = 5, message = "sts_header_overrides supports a maximum of 5 headers to override")
    private Map<String, String> awsStsHeaderOverrides;

    @JsonProperty("serverless")
    private Boolean serverless = false;

    @JsonProperty("serverless_options")
    private ServerlessOptions serverlessOptions;

    public String getAwsStsRoleArn() {
        return awsStsRoleArn;
    }

    public String getAwsStsExternalId() {
        return awsStsExternalId;
    }

    public Region getAwsRegion() {
        return awsRegion != null ? Region.of(awsRegion) : null;
    }

    public Map<String, String> getAwsStsHeaderOverrides() {
        return awsStsHeaderOverrides;
    }

    public Boolean isServerlessCollection() {
        return serverless;
    }

    public ServerlessOptions getServerlessOptions() {
        return serverlessOptions;
    }
}

