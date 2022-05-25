/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.UUID;

public class AwsAuthenticationOptions {
    @JsonProperty("aws_region")
    @NotBlank(message = "Region cannot be null or empty")
    private String awsRegion;

    @JsonProperty("aws_sts_role_arn")
    @Size(min = 20, max = 2048, message = "awsStsRoleArn length should be between 1 and 2048 characters")
    private String awsStsRoleArn;

    public String getAwsRegion() {
        return awsRegion;
    }

    public String getAwsStsRoleArn() {
        return awsStsRoleArn;
    }

    public AwsCredentialsProvider authenticateAwsConfiguration(final StsClient stsClient) {

        AwsCredentialsProvider awsCredentialsProvider;
        if (awsStsRoleArn != null && !awsStsRoleArn.isEmpty()) {
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

