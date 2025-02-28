/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.ExportConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

public class ClientFactory {

    private final AwsCredentialsProvider awsCredentialsProvider;
    private final AwsAuthenticationConfig awsAuthenticationConfig;
    private final ExportConfig exportConfig;

    public ClientFactory(final AwsCredentialsSupplier awsCredentialsSupplier,
                         final AwsAuthenticationConfig awsAuthenticationConfig,
                         final ExportConfig exportConfig) {
        awsCredentialsProvider = awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder()
                .withRegion(awsAuthenticationConfig.getAwsRegion())
                .withStsRoleArn(awsAuthenticationConfig.getAwsStsRoleArn())
                .withStsExternalId(awsAuthenticationConfig.getAwsStsExternalId())
                .withStsHeaderOverrides(awsAuthenticationConfig.getAwsStsHeaderOverrides())
                .build());
        this.awsAuthenticationConfig = awsAuthenticationConfig;
        this.exportConfig = exportConfig;
    }


    public DynamoDbStreamsClient buildDynamoDbStreamClient() {
        DynamoDbStreamsClientBuilder clientBuilder = DynamoDbStreamsClient.builder()
                .credentialsProvider(awsCredentialsProvider)
                .region(awsAuthenticationConfig.getAwsRegion());

        if (awsAuthenticationConfig.getEndpointUrl() != null && !awsAuthenticationConfig.getEndpointUrl().isEmpty()) {
            clientBuilder.endpointOverride(URI.create(awsAuthenticationConfig.getEndpointUrl()));
        }

        return clientBuilder.build();
    }


    public DynamoDbClient buildDynamoDBClient() {
        DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder()
                .region(awsAuthenticationConfig.getAwsRegion())
                .credentialsProvider(awsCredentialsProvider);

        if (awsAuthenticationConfig.getEndpointUrl() != null && !awsAuthenticationConfig.getEndpointUrl().isEmpty()) {
            clientBuilder.endpointOverride(URI.create(awsAuthenticationConfig.getEndpointUrl()));
        }

        return clientBuilder.build();
    }


    public S3Client buildS3Client() {
        return S3Client.builder()
                .region(getS3ClientRegion())
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(retryPolicy -> retryPolicy.numRetries(5).build())
                        .build())
                .build();
    }

    private Region getS3ClientRegion() {
        if (exportConfig != null && exportConfig.getAwsRegion() != null) {
            return exportConfig.getAwsRegion();
        }

        return awsAuthenticationConfig.getAwsRegion();
    }

}
