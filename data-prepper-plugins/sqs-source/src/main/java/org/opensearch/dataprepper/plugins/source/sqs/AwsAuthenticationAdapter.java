/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

class AwsAuthenticationAdapter {
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final SqsSourceConfig sqsSourceConfig;


    AwsAuthenticationAdapter(
            final AwsCredentialsSupplier awsCredentialsSupplier,
            final SqsSourceConfig sqsSourceConfig) {
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.sqsSourceConfig = sqsSourceConfig;
    }

    AwsCredentialsProvider getCredentialsProvider() {
        final AwsAuthenticationOptions awsAuthenticationOptions = sqsSourceConfig.getAwsAuthenticationOptions();

        final AwsCredentialsOptions options = AwsCredentialsOptions.builder()
                .withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn())
                .withRegion(awsAuthenticationOptions.getAwsRegion())
                .withStsHeaderOverrides(awsAuthenticationOptions.getAwsStsHeaderOverrides())
                .withStsExternalId(awsAuthenticationOptions.getAwsStsExternalId())
                .build();

        return awsCredentialsSupplier.getProvider(options);
    }
}
