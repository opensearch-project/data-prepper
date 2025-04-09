/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.xrayotlp.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import software.amazon.awssdk.regions.Region;

/**
 * Configuration class for the X-Ray OTLP sink plugin.
 * This class defines the configuration options available when setting up
 * the X-Ray OTLP sink in Data Prepper pipelines.
 * This class will be automatically wired by Data-Prepper; the Builder is only for testing.
 *
 * @since 2.6
 */
@Builder
public class XRayOTLPSinkConfig {
    /**
     * AWS configuration for X-Ray access.
     * Contains authentication and region settings required for AWS X-Ray service.
     * This is a required configuration and must be valid.
     */
    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationConfiguration awsAuthenticationConfiguration;

    public Region getAwsRegion() {
        return awsAuthenticationConfiguration.getAwsRegion();
    }

    public String getStsRoleArn() {
        return awsAuthenticationConfiguration.getAwsStsRoleArn();
    }

    public String getStsExternalId() {
        return awsAuthenticationConfiguration.getAwsStsExternalId();
    }
}
