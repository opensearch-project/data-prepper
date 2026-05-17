/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.s3_enrich.processor.client;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.s3.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.S3EnrichProcessorConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ClientFactory {

    public static S3Client createS3Client(final S3EnrichProcessorConfig s3EnrichProcessorConfig, final AwsCredentialsSupplier awsCredentialsSupplier) {
        final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(
                s3EnrichProcessorConfig.getAwsAuthenticationOptions());
        final Region region = s3EnrichProcessorConfig.getAwsAuthenticationOptions().getAwsRegion();
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