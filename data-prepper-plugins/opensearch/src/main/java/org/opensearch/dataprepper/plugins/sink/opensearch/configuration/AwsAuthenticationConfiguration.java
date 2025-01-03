/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import software.amazon.awssdk.regions.Region;

import java.util.Map;

import static org.opensearch.dataprepper.plugins.sink.opensearch.configuration.OpenSearchSinkConfig.DEFAULT_AWS_REGION;

public class AwsAuthenticationConfiguration {

    @JsonProperty("region")
    @Size(min = 1, message = "Region cannot be empty string")
    private String awsRegion = DEFAULT_AWS_REGION;

    @JsonProperty("sts_role_arn")
    @Size(min = 20, max = 2048, message = "awsStsRoleArn length should be between 1 and 2048 characters")
    private String awsStsRoleArn;

    @JsonProperty("sts_external_id")
    @Size(min = 2, max = 1224, message = "awsStsExternalId length should be between 2 and 1224 characters")
    private String awsStsExternalId;

    @JsonProperty("sts_header_overrides")
    @Size(max = 5, message = "sts_header_overrides supports a maximum of 5 headers to override")
    private Map<String, String> awsStsHeaderOverrides;

    @JsonProperty("serverless")
    private boolean serverless = false;

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

    public boolean isServerlessCollection() {
        return serverless;
    }

    public ServerlessOptions getServerlessOptions() {
        return serverlessOptions;
    }
}

