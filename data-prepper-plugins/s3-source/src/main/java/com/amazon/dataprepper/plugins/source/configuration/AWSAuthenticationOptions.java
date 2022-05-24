/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;

public class AWSAuthenticationOptions {
    @JsonProperty("aws_region")
    @NotBlank(message = "Region cannot be null or empty")
    private String awsRegion;

    @JsonProperty("aws_sts_role_arn")
    private String awsStsRoleArn;

    public String getAwsRegion() {
        return awsRegion;
    }

    public String getAwsStsRoleArn() {
        return awsStsRoleArn;
    }

    public AwsCredentialsProvider authenticateAWSConfiguration(final StsClient stsClient) {

        AwsCredentialsProvider awsCredentialsProvider;
        if (awsStsRoleArn != null && !awsStsRoleArn.isEmpty()) {
            checkArgument(awsStsRoleArn.length() <= 2048, "awsStsRoleArn length cannot exceed 2048");
            try {
                Arn.fromString(awsStsRoleArn);
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid ARN format for awsStsRoleArn");
            }

            awsCredentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(AssumeRoleRequest.builder()
                            .roleSessionName("S3-Source-" + UUID.randomUUID())
                            .roleArn(awsStsRoleArn)
                            .build())
                    .build();

        }
        else {
            // use default credential provider
            awsCredentialsProvider = DefaultCredentialsProvider.create();
        }

        return awsCredentialsProvider;
    }
}

