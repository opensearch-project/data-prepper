/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.s3.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

class AwsAuthenticationAdapter {
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final S3SourceConfig s3SourceConfig;


    AwsAuthenticationAdapter(
            final AwsCredentialsSupplier awsCredentialsSupplier,
            final S3SourceConfig s3SourceConfig) {
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.s3SourceConfig = s3SourceConfig;
    }

    AwsCredentialsProvider getCredentialsProvider() {
        final AwsAuthenticationOptions awsAuthenticationOptions = s3SourceConfig.getAwsAuthenticationOptions();

        final AwsCredentialsOptions options = AwsCredentialsOptions.builder()
                .withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn())
                .withRegion(awsAuthenticationOptions.getAwsRegion())
                .withStsHeaderOverrides(awsAuthenticationOptions.getAwsStsHeaderOverrides())
                .withStsExternalId(awsAuthenticationOptions.getAwsStsExternalId())
                .build();

        return awsCredentialsSupplier.getProvider(options);
    }
}
