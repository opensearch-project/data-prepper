/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.AwsAuthenticationConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.s3.S3Client;

public class ClientFactory {

    private final AwsCredentialsProvider awsCredentialsProvider;

    public ClientFactory(AwsCredentialsSupplier awsCredentialsSupplier, AwsAuthenticationConfig awsAuthenticationConfig) {
        awsCredentialsProvider = awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder()
                .withRegion(awsAuthenticationConfig.getAwsRegion())
                .withStsRoleArn(awsAuthenticationConfig.getAwsStsRoleArn())
                .withStsExternalId(awsAuthenticationConfig.getAwsStsExternalId())
                .withStsHeaderOverrides(awsAuthenticationConfig.getAwsStsHeaderOverrides())
                .build());
    }


    public DynamoDbStreamsClient buildDynamoDbStreamClient() {
        return DynamoDbStreamsClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }


    public DynamoDbClient buildDynamoDBClient() {
        return DynamoDbClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }


    public S3Client buildS3Client() {
        return S3Client.builder()
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }

}
