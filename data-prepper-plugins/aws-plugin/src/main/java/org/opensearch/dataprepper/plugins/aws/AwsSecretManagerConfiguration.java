/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.hibernate.validator.constraints.time.DurationMin;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class AwsSecretManagerConfiguration {
    static final String DEFAULT_AWS_REGION = "us-east-1";
    private static final String AWS_IAM_ROLE = "role";
    private static final String AWS_IAM = "iam";

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

    public SecretsManagerClient createSecretManagerClient() {
        return SecretsManagerClient.builder()
                .credentialsProvider(authenticateAwsConfiguration())
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

    private AwsCredentialsProvider authenticateAwsConfiguration() {

        final AwsCredentialsProvider awsCredentialsProvider;
        if (awsStsRoleArn != null && !awsStsRoleArn.isEmpty()) {

            validateStsRoleArn();

            final StsClient stsClient = StsClient.builder()
                    .region(getAwsRegion())
                    .build();

            AssumeRoleRequest.Builder assumeRoleRequestBuilder = AssumeRoleRequest.builder()
                    .roleSessionName("aws-secret-" + UUID.randomUUID())
                    .roleArn(awsStsRoleArn);

            if (awsStsHeaderOverrides != null && !awsStsHeaderOverrides.isEmpty()) {
                assumeRoleRequestBuilder = assumeRoleRequestBuilder.overrideConfiguration(
                        configuration -> awsStsHeaderOverrides.forEach(configuration::putHeader));
            }

            awsCredentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(assumeRoleRequestBuilder.build())
                    .build();

        } else {
            // use default credential provider
            awsCredentialsProvider = DefaultCredentialsProvider.create();
        }

        return awsCredentialsProvider;
    }

    private void validateStsRoleArn() {
        final Arn arn = getArn();
        if (!AWS_IAM.equals(arn.service())) {
            throw new IllegalArgumentException("sts_role_arn must be an IAM Role");
        }
        final Optional<String> resourceType = arn.resource().resourceType();
        if (resourceType.isEmpty() || !resourceType.get().equals(AWS_IAM_ROLE)) {
            throw new IllegalArgumentException("sts_role_arn must be an IAM Role");
        }
    }

    private Arn getArn() {
        try {
            return Arn.fromString(awsStsRoleArn);
        } catch (final Exception e) {
            throw new IllegalArgumentException(String.format("Invalid ARN format for sts_role_arn. Check the format of %s", awsStsRoleArn));
        }
    }
}
