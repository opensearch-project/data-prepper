/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.hibernate.validator.constraints.time.DurationMin;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueRequest;

import java.time.Duration;
import java.util.Map;

public class AwsSecretManagerConfiguration {
    static final String DEFAULT_AWS_REGION = "us-east-1";

    @JsonProperty("secret_id")
    @NotNull
    @Size(min = 1, max = 512, message = "awsSecretId length should be between 1 and 512 characters")
    private String awsSecretId;

    @JsonProperty("region")
    @Size(min = 1, message = "Region cannot be empty string")
    private String awsRegion = DEFAULT_AWS_REGION;

    @JsonProperty("sts_role_arn")
    @Size(min = 20, max = 2048, message = "awsStsRoleArn length should be between 1 and 2048 characters")
    private String awsStsRoleArn;

    @JsonProperty("sts_header_overrides")
    @Size(max = 5, message = "sts_header_overrides supports a maximum of 5 headers to override")
    private Map<String, String> awsStsHeaderOverrides;

    @JsonProperty("refresh_interval")
    @NotNull(message = "refresh_interval must not be null")
    @DurationMin(hours = 1L, message = "Refresh interval must be at least 1 hour.")
    private Duration refreshInterval = Duration.ofHours(1L);

    @JsonProperty("disable_refresh")
    private boolean disableRefresh = false;

    public String getAwsSecretId() {
        return awsSecretId;
    }

    public Region getAwsRegion() {
        return Region.of(awsRegion);
    }

    public Duration getRefreshInterval() {
        return refreshInterval;
    }

    public boolean isDisableRefresh() {
        return disableRefresh;
    }

    public SecretsManagerClient createSecretManagerClient(final AwsCredentialsSupplier awsCredentialsSupplier) {
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder()
                .withRegion(this.awsRegion)
                .withStsRoleArn(this.awsStsRoleArn)
                .withStsHeaderOverrides(this.awsStsHeaderOverrides)
                .build());

        return SecretsManagerClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(getAwsRegion())
                .build();
    }

    public GetSecretValueRequest createGetSecretValueRequest() {
        return GetSecretValueRequest.builder()
                .secretId(awsSecretId)
                .build();
    }

    public PutSecretValueRequest putSecretValueRequest(String secretKeyValueMapAsString) {
        return PutSecretValueRequest.builder()
                .secretId(awsSecretId)
                .secretString(secretKeyValueMapAsString)
                .build();
    }
}
