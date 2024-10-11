/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.rds.configuration.AwsAuthenticationConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.s3.S3Client;

public class ClientFactory {
    private final AwsAuthenticationConfig awsAuthenticationConfig;
    private final AwsCredentialsProvider awsCredentialsProvider;
    private final RdsSourceConfig sourceConfig;

    public ClientFactory(final AwsCredentialsSupplier awsCredentialsSupplier,
                         final RdsSourceConfig sourceConfig) {
        awsAuthenticationConfig = sourceConfig.getAwsAuthenticationConfig();
        awsCredentialsProvider = awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder()
                .withRegion(awsAuthenticationConfig.getAwsRegion())
                .withStsRoleArn(awsAuthenticationConfig.getAwsStsRoleArn())
                .withStsExternalId(awsAuthenticationConfig.getAwsStsExternalId())
                .withStsHeaderOverrides(awsAuthenticationConfig.getAwsStsHeaderOverrides())
                .build());
        this.sourceConfig = sourceConfig;
    }

    public RdsClient buildRdsClient() {
        return RdsClient.builder()
                .region(awsAuthenticationConfig.getAwsRegion())
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }

    public S3Client buildS3Client() {
        return S3Client.builder()
                .region(getS3ClientRegion())
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }

    private Region getS3ClientRegion() {
        if (sourceConfig.getS3Region() != null) {
            return sourceConfig.getS3Region();
        }

        return awsAuthenticationConfig.getAwsRegion();
    }
}
