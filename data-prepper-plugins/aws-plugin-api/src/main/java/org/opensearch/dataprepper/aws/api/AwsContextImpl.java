/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.aws.api;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.UUID;

public class AwsContextImpl implements AwsContext {
    private final AwsCredentialsConfig awsCredentialsConfig;

    public AwsContextImpl(final AwsCredentialsConfig awsCredentialsConfig) {
        this.awsCredentialsConfig = awsCredentialsConfig;
    }

    @Override
    public AwsCredentialsProvider getOrDefault() {
        if (awsCredentialsConfig == null || awsCredentialsConfig.getStsRoleArn() == null) {
            return getDefault();
        }

        return getFromOptions(awsCredentialsConfig.toCredentialsOptions());
    }

    @Override
    public Region getRegionOrDefault() {
        if (awsCredentialsConfig != null && awsCredentialsConfig.getRegion() != null) {
            return Region.of(awsCredentialsConfig.getRegion());
        }
        return null;
    }

    private AwsCredentialsProvider getDefault() {
        final AwsCredentialsOptions credentialsOptions;
        if (awsCredentialsConfig != null) {
            credentialsOptions = awsCredentialsConfig.toCredentialsOptions();
        } else {
            credentialsOptions = AwsCredentialsOptions.defaultOptions();
        }

        return getFromOptions(credentialsOptions);
    }

    private AwsCredentialsProvider getFromOptions(AwsCredentialsOptions awsCredentialsOptions) {
        final AwsCredentialsProvider awsCredentialsProvider;
        final String awsStsRoleArn = awsCredentialsOptions.getStsRoleArn();
        if (awsStsRoleArn != null && !awsStsRoleArn.isEmpty()) {

            final StsClient stsClient = StsClient.builder()
                    .region(awsCredentialsOptions.getRegion())
                    .build();

            AssumeRoleRequest.Builder assumeRoleRequestBuilder = AssumeRoleRequest.builder()
                    .roleSessionName("aws-iam-" + UUID.randomUUID())
                    .roleArn(awsStsRoleArn);

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
