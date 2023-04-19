/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.configuration;

import java.util.Map;
import java.util.UUID;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Size;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

/**
 * An implementation class AWS Authentication configuration
 */
public class AwsAuthenticationOptions {
    @JsonProperty("region")
    @Size(min = 1, message = "Region cannot be empty string")
    private String awsRegion;

    @JsonProperty("sts_role_arn")
    @Size(min = 20, max = 2048, message = "awsStsRoleArn length should be between 1 and 2048 characters")
    private String awsStsRoleArn;

    @JsonProperty("sts_header_overrides")
    @Size(max = 5, message = "sts_header_overrides supports a maximum of 5 headers to override")
    private Map<String, String> awsStsHeaderOverrides;

    /**
     * AWS Region configuration
     */
    public Region getAwsRegion() {
        return awsRegion != null ? Region.of(awsRegion) : null;
    }

    /**
     * Aws Credentials Provider configuration
     */
    public AwsCredentialsProvider authenticateAwsConfiguration() {

        final AwsCredentialsProvider awsCredentialsProvider;
        if (awsStsRoleArn != null && !awsStsRoleArn.isEmpty()) {
            try {
                Arn.fromString(awsStsRoleArn);
            } catch (final Exception e) {
                throw new IllegalArgumentException("Invalid ARN format for awsStsRoleArn");
            }

            final StsClient stsClient = StsClient.builder().region(getAwsRegion()).build();

            AssumeRoleRequest.Builder assumeRoleRequestBuilder = AssumeRoleRequest.builder()
                    .roleSessionName("S3-Sink-" + UUID.randomUUID()).roleArn(awsStsRoleArn);

            if (awsStsHeaderOverrides != null && !awsStsHeaderOverrides.isEmpty()) {
                assumeRoleRequestBuilder = assumeRoleRequestBuilder.overrideConfiguration(
                        configuration -> awsStsHeaderOverrides.forEach(configuration::putHeader));
            }

            awsCredentialsProvider = StsAssumeRoleCredentialsProvider.builder().stsClient(stsClient)
                    .refreshRequest(assumeRoleRequestBuilder.build()).build();

        } else {
            awsCredentialsProvider = DefaultCredentialsProvider.create();
        }
        return awsCredentialsProvider;
    }
}