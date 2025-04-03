/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.xrayotlp;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.sink.xrayotlp.configuration.AwsAuthenticationConfiguration;

/**
 * Configuration class for the X-Ray OTLP sink plugin.
 * This class defines the configuration options available when setting up
 * the X-Ray OTLP sink in Data Prepper pipelines.
 *
 * @since 2.6
 */
public class XRayOTLPSinkConfig {
    public static final String DEFAULT_AWS_REGION = "us-east-1";

    /**
     * AWS configuration for X-Ray access.
     * Contains authentication and region settings required for AWS X-Ray service.
     * This is a required configuration and must be valid.
     */
    @Getter
    @JsonProperty("aws")
    @NotNull
    @Valid
    AwsAuthenticationConfiguration awsAuthenticationConfiguration;
}
