/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.personalize;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.personalize.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.personalize.configuration.PersonalizeSinkConfiguration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.personalizeevents.PersonalizeEventsClient;

final class ClientFactory {
    private ClientFactory() { }

    static PersonalizeEventsClient createPersonalizeEventsClient(final PersonalizeSinkConfiguration personalizeSinkConfig, final AwsCredentialsSupplier awsCredentialsSupplier) {
        final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(personalizeSinkConfig.getAwsAuthenticationOptions());
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);

        return PersonalizeEventsClient.builder()
                .region(getRegion(personalizeSinkConfig, awsCredentialsSupplier))
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(createOverrideConfiguration(personalizeSinkConfig)).build();
    }

    private static ClientOverrideConfiguration createOverrideConfiguration(final PersonalizeSinkConfiguration personalizeSinkConfig) {
        final RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(personalizeSinkConfig.getMaxRetries()).build();
        return ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .build();
    }

    private static AwsCredentialsOptions convertToCredentialsOptions(final AwsAuthenticationOptions awsAuthenticationOptions) {
        if (awsAuthenticationOptions == null) {
            return AwsCredentialsOptions.builder().build();
        }
        return AwsCredentialsOptions.builder()
                .withRegion(awsAuthenticationOptions.getAwsRegion().orElse(null))
                .withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn())
                .withStsExternalId(awsAuthenticationOptions.getAwsStsExternalId())
                .withStsHeaderOverrides(awsAuthenticationOptions.getAwsStsHeaderOverrides())
                .build();
    }

    private static Region getRegion(final PersonalizeSinkConfiguration personalizeSinkConfig, final AwsCredentialsSupplier awsCredentialsSupplier) {
        Region defaultRegion = awsCredentialsSupplier.getDefaultRegion().orElse(null);
        if (personalizeSinkConfig.getAwsAuthenticationOptions() == null) {
            return defaultRegion;
        } else {
            return personalizeSinkConfig.getAwsAuthenticationOptions().getAwsRegion().orElse(defaultRegion);
        }
    }
}