/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class AwsAuthenticationAdapter {
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final S3SourceConfig s3SourceConfig;


    public AwsAuthenticationAdapter(
            AwsCredentialsSupplier awsCredentialsSupplier,
            S3SourceConfig s3SourceConfig) {
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.s3SourceConfig = s3SourceConfig;
    }

    public AwsCredentialsProvider getCredentialsProvider() {
        final AwsAuthenticationOptions awsAuthenticationOptions = s3SourceConfig.getAwsAuthenticationOptions();

        AwsCredentialsOptions options = AwsCredentialsOptions.builder()
                .withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn())
                .withRegion(awsAuthenticationOptions.getAwsRegion())
                .withStsHeaderOverrides(s3SourceConfig.getAwsAuthenticationOptions().getAwsStsHeaderOverrides())
                .build();

        return awsCredentialsSupplier.getProvider(options);
    }
}
