/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.s3_enricher.processor;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public class AwsAuthenticationAdapter {
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final S3EnricherProcessorConfig s3EnricherProcessorConfig;


    AwsAuthenticationAdapter(
            final AwsCredentialsSupplier awsCredentialsSupplier,
            final S3EnricherProcessorConfig s3EnricherProcessorConfig) {
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.s3EnricherProcessorConfig = s3EnricherProcessorConfig;
    }

    AwsCredentialsProvider getCredentialsProvider() {
        final AwsAuthenticationOptions awsAuthenticationOptions = s3EnricherProcessorConfig.getAwsAuthenticationOptions();

        final AwsCredentialsOptions options = AwsCredentialsOptions.builder()
                .withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn())
                .withRegion(awsAuthenticationOptions.getAwsRegion())
                .withStsHeaderOverrides(awsAuthenticationOptions.getAwsStsHeaderOverrides())
                .withStsExternalId(awsAuthenticationOptions.getAwsStsExternalId())
                .build();

        return awsCredentialsSupplier.getProvider(options);
    }
}
