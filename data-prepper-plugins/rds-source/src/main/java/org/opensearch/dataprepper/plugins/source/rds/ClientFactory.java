/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.rds.configuration.AwsAuthenticationConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.s3.S3Client;

public class ClientFactory {
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final AwsAuthenticationConfig awsAuthenticationConfig;

    public ClientFactory(final AwsCredentialsSupplier awsCredentialsSupplier,
                         final AwsAuthenticationConfig awsAuthenticationConfig) {
        awsCredentialsProvider = awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder()
                .withRegion(awsAuthenticationConfig.getAwsRegion())
                .withStsRoleArn(awsAuthenticationConfig.getAwsStsRoleArn())
                .withStsExternalId(awsAuthenticationConfig.getAwsStsExternalId())
                .withStsHeaderOverrides(awsAuthenticationConfig.getAwsStsHeaderOverrides())
                .build());
        this.awsAuthenticationConfig = awsAuthenticationConfig;
    }

    public RdsClient buildRdsClient() {
        return RdsClient.builder()
                .region(awsAuthenticationConfig.getAwsRegion())
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }

    public S3Client buildS3Client() {
        return S3Client.builder()
                .region(awsAuthenticationConfig.getAwsRegion())
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }
}
