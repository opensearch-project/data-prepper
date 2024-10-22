/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.kinesis.source.configuration.AwsAuthenticationConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.KinesisClientUtil;

public class KinesisClientFactory {
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final AwsCredentialsProvider defaultCredentialsProvider;
    private final AwsAuthenticationConfig awsAuthenticationConfig;

    public KinesisClientFactory(final AwsCredentialsSupplier awsCredentialsSupplier,
                                final AwsAuthenticationConfig awsAuthenticationConfig) {
        awsCredentialsProvider = awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder()
                .withRegion(awsAuthenticationConfig.getAwsRegion())
                .withStsRoleArn(awsAuthenticationConfig.getAwsStsRoleArn())
                .withStsExternalId(awsAuthenticationConfig.getAwsStsExternalId())
                .withStsHeaderOverrides(awsAuthenticationConfig.getAwsStsHeaderOverrides())
                .build());
        defaultCredentialsProvider = awsCredentialsSupplier.getProvider(
                AwsCredentialsOptions.defaultOptionsWithDefaultCreds());
        this.awsAuthenticationConfig = awsAuthenticationConfig;
    }

    public DynamoDbAsyncClient buildDynamoDBClient(Region region) {
        return DynamoDbAsyncClient.builder()
                .credentialsProvider(defaultCredentialsProvider)
                .region(region)
                .build();
    }

    public KinesisAsyncClient buildKinesisAsyncClient(Region region) {
        return KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient.builder()
                    .credentialsProvider(awsCredentialsProvider)
                        .region(region)
        );
    }

    public CloudWatchAsyncClient buildCloudWatchAsyncClient(Region region) {
        return CloudWatchAsyncClient.builder()
                .credentialsProvider(defaultCredentialsProvider)
                .region(region)
                .build();
    }
}
