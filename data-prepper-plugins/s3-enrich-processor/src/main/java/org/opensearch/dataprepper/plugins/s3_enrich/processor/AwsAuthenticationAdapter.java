/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.s3_enrich.processor;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.s3.common.config.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class AwsAuthenticationAdapter {
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final S3EnrichProcessorConfig s3EnrichProcessorConfig;


    AwsAuthenticationAdapter(
            final AwsCredentialsSupplier awsCredentialsSupplier,
            final S3EnrichProcessorConfig s3EnricherProcessorConfig) {
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.s3EnrichProcessorConfig = s3EnricherProcessorConfig;
    }

    AwsCredentialsProvider getCredentialsProvider() {
        final AwsAuthenticationOptions awsAuthenticationOptions = s3EnrichProcessorConfig.getAwsAuthenticationOptions();

        final AwsCredentialsOptions options = AwsCredentialsOptions.builder()
                .withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn())
                .withRegion(awsAuthenticationOptions.getAwsRegion())
                .withStsHeaderOverrides(awsAuthenticationOptions.getAwsStsHeaderOverrides())
                .withStsExternalId(awsAuthenticationOptions.getAwsStsExternalId())
                .build();

        return awsCredentialsSupplier.getProvider(options);
    }
}
