/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class AwsAuthenticationOptions {
    private static final String AWS_IAM_ROLE = "role";
    private static final String AWS_IAM = "iam";

    @JsonProperty("region")
    @Size(min = 1, message = "Region cannot be empty string")
    private String awsRegion;

    @JsonProperty("sts_role_arn")
    @Size(min = 20, max = 2048, message = "awsStsRoleArn length should be between 1 and 2048 characters")
    private String awsStsRoleArn;

    @JsonProperty("sts_header_overrides")
    @Size(max = 5, message = "sts_header_overrides supports a maximum of 5 headers to override")
    private Map<String, String> awsStsHeaderOverrides;

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
            throw new IllegalArgumentException(String.format("Invalid ARN format for awsStsRoleArn. Check the format of %s", awsStsRoleArn));
        }
    }

    public String getAwsStsRoleArn() {
        return awsStsRoleArn;
    }

    public Region getAwsRegion() {
        return awsRegion != null ? Region.of(awsRegion) : null;
    }

    public AwsCredentialsProvider authenticateAwsConfiguration() {

        final AwsCredentialsProvider awsCredentialsProvider;
        if (awsStsRoleArn != null && !awsStsRoleArn.isEmpty()) {

            validateStsRoleArn();

            final StsClient stsClient = StsClient.builder()
                    .region(getAwsRegion())
                    .build();

            AssumeRoleRequest.Builder assumeRoleRequestBuilder = AssumeRoleRequest.builder()
                    .roleSessionName("S3-Source-" + UUID.randomUUID())
                    .roleArn(awsStsRoleArn);
            if(awsStsHeaderOverrides != null && !awsStsHeaderOverrides.isEmpty()) {
                assumeRoleRequestBuilder = assumeRoleRequestBuilder
                        .overrideConfiguration(configuration -> awsStsHeaderOverrides.forEach(configuration::putHeader));
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
}

