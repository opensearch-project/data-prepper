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
import org.opensearch.dataprepper.aws.api.AwsConfig;
import software.amazon.awssdk.regions.Region;

import java.net.URI;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

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
     * This field is kept private and its contents should be accessed via the generated getter methods.
     */
    @JsonProperty("aws")
    @Valid
    private AwsConfig awsConfig;

    /**
     * Get AWS region from the provided endpoint.
     *
     * @return the AWS region
     */
    public Region getAwsRegion() {
        try {
            final String host = URI.create(this.endpoint).getHost();
            if (host == null) {
                throw new IllegalArgumentException();
            }

            final Set<String> knownRegions = Region.regions().stream()
                    .map(Region::id)
                    .collect(Collectors.toSet());

            return Arrays.stream(host.split("\\."))
                    .filter(knownRegions::contains)
                    .map(Region::of)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("No valid AWS region found in endpoint: " + endpoint));
        } catch (final Exception e) {
            throw new IllegalArgumentException("Failed to parse AWS region from endpoint: " + endpoint, e);
        }
    }

    public String getStsRoleArn() {
        if (awsConfig == null || awsConfig.getAwsStsRoleArn() == null) {
            return null;
        }

        return awsConfig.getAwsStsRoleArn();
    }

    public String getStsExternalId() {
        if (awsConfig == null || awsConfig.getAwsStsExternalId() == null) {
            return null;
        }

        return awsConfig.getAwsStsExternalId();
    }

    /**
     * Validate the AWS configuration.
     * This method ensures breaking change in future release where non-AWS OTLP endpoints are supported.
     *
     * @throws IllegalArgumentException if the AWS configuration is invalid
     */
    public void validate() {
        if (awsConfig == null) {
            throw new IllegalArgumentException("aws configuration is required");
        }
    }
}
