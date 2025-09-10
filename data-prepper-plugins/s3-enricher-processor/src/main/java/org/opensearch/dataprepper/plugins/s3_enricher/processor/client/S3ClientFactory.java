/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.s3_enricher.processor.client;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.S3EnricherProcessorConfig;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ClientFactory {

    public static S3Client createS3Client(final S3EnricherProcessorConfig s3EnricherProcessorConfig, final AwsCredentialsSupplier awsCredentialsSupplier) {
        final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(
                s3EnricherProcessorConfig.getAwsAuthenticationOptions());
        final Region region = s3EnricherProcessorConfig.getAwsAuthenticationOptions().getAwsRegion();
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(
                awsCredentialsOptions);

        return S3Client.builder()
                .region(region)
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(retryPolicy -> retryPolicy.numRetries(5).build())
                        .build())
                .build();
    }

    public static AwsCredentialsOptions convertToCredentialsOptions(
            final AwsAuthenticationOptions awsAuthenticationOptions) {
        if (awsAuthenticationOptions == null || awsAuthenticationOptions.getAwsStsRoleArn() == null) {
            return AwsCredentialsOptions.defaultOptionsWithDefaultCredentialsProvider();
        }
        return AwsCredentialsOptions.builder()
                .withRegion(awsAuthenticationOptions.getAwsRegion())
                .withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn())
                .withStsExternalId(awsAuthenticationOptions.getAwsStsExternalId())
                .withStsHeaderOverrides(awsAuthenticationOptions.getAwsStsHeaderOverrides())
                .build();
    }
}