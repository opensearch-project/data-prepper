/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;
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
 *
 * @since 2.6
 */
@NoArgsConstructor
public class OtlpSinkConfig {

    @Getter
    @JsonProperty("endpoint")
    @Size(min = 1, message = "endpoint cannot be empty string")
    private String endpoint;

    @Getter
    @JsonProperty("batch_size")
    @Min(value = 10, message = "batch_size must be at least 10")
    @Max(value = 512, message = "batch_size must be at most 512")
    private int batchSize = 100;

    @Getter
    @JsonProperty("max_retries")
    @Min(value = 1, message = "max_retries must be at least 1")
    @Max(value = 5, message = "max_retries must be at most 5")
    private int maxRetries = 3;

    /**
     * AWS authentication configuration.
     * This object contains the AWS region and STS role ARN (if applicable).
     * This field is kept private and its contents should be accessed via the generated getter methods.
     */
    @JsonProperty("aws")
    @Valid
    private AwsAuthenticationConfiguration awsAuthenticationConfiguration;

    public Region getAwsRegion() {
        if (awsAuthenticationConfiguration == null) {
            return null;
        }

        return awsAuthenticationConfiguration.getAwsRegion();
    }

    public String getStsRoleArn() {
        if (awsAuthenticationConfiguration == null) {
            return null;
        }

        return awsAuthenticationConfiguration.getAwsStsRoleArn();
    }

    public String getStsExternalId() {
        if (awsAuthenticationConfiguration == null) {
            return null;
        }

        return awsAuthenticationConfiguration.getAwsStsExternalId();
    }
}
