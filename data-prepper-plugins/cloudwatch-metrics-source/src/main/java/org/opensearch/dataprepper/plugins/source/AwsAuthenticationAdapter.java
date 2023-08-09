/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * An implementation class for getting AWS authentication credentials
 */
class AwsAuthenticationAdapter {
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final CloudwatchMetricsSourceConfig cloudwatchMetricsSourceConfig;

    AwsAuthenticationAdapter(
            final AwsCredentialsSupplier awsCredentialsSupplier,
            final CloudwatchMetricsSourceConfig cloudwatchMetricsSourceConfig) {
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.cloudwatchMetricsSourceConfig = cloudwatchMetricsSourceConfig;
    }

    /**
     * Get the Aws Credentials Provider
     * @return AwsCredentialsProvider
     */
    AwsCredentialsProvider getCredentialsProvider() {
        final AwsAuthenticationOptions awsAuthenticationOptions = cloudwatchMetricsSourceConfig.getAwsAuthenticationOptions();

        final AwsCredentialsOptions options = AwsCredentialsOptions.builder()
                .withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn())
                .withRegion(awsAuthenticationOptions.getAwsRegion())
                .build();

        return awsCredentialsSupplier.getProvider(options);
    }
}
