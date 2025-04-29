/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.regions.Region;

/**
 * Configuration class for the OTLP sink plugin.
 * This class defines the configuration options available when setting up
 * the OTLP sink in Data Prepper pipelines.
 * <p>
 * Note that {@code @Getter} is applied at the field level (not the class level)
 * to preserve encapsulation and maintain control over exposed configuration data.
 * <p>
 * This class is automatically wired by the Data Prepper framework during pipeline initialization.
 */
@NoArgsConstructor
public class OtlpSinkConfig {

    @Getter
    @JsonProperty("endpoint")
    @NotBlank(message = "endpoint is required")
    private String endpoint;

    @Getter
    @JsonProperty("max_retries")
    @Min(value = 0)
    private int maxRetries = 5;

    /**
     * The threshold configuration for sending spans to the OTLP endpoint.
     * This field is kept private and its contents should be accessed via the generated getter methods.
     * Using eager-default values and allows the configuration to be optional in the pipeline configuration.
     */
    @JsonProperty("threshold")
    @Valid
    private ThresholdConfig thresholdConfig = new ThresholdConfig();

    public int getMaxEvents() {
        return thresholdConfig.getMaxEvents();
    }

    public long getMaxBatchSize() {
        return thresholdConfig.getMaxBatchSize().getBytes();
    }

    public long getFlushTimeoutMillis() {
        return thresholdConfig.getFlushTimeout().toMillis();
    }

    /**
     * AWS authentication configuration.
     * This object contains the AWS region and STS role ARN (if applicable).
     * This field is kept private and its contents should be accessed via the generated getter methods.
     */
    @JsonProperty("aws")
    @Valid
    private AwsAuthenticationConfig awsAuthenticationConfig;

    public Region getAwsRegion() {
        if (awsAuthenticationConfig == null) {
            return null;
        }

        return awsAuthenticationConfig.getAwsRegion();
    }

    public String getStsRoleArn() {
        if (awsAuthenticationConfig == null) {
            return null;
        }

        return awsAuthenticationConfig.getAwsStsRoleArn();
    }

    public String getStsExternalId() {
        if (awsAuthenticationConfig == null) {
            return null;
        }

        return awsAuthenticationConfig.getAwsStsExternalId();
    }
}
