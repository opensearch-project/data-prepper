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
import software.amazon.awssdk.services.personalizeevents.PersonalizeEventsClient;

public final class ClientFactory {
    private ClientFactory() { }

    static PersonalizeEventsClient createPersonalizeEventsClient(final PersonalizeSinkConfiguration personalizeSinkConfig, final AwsCredentialsSupplier awsCredentialsSupplier) {
        final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(personalizeSinkConfig.getAwsAuthenticationOptions());
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);

        return PersonalizeEventsClient.builder()
                .region(personalizeSinkConfig.getAwsAuthenticationOptions().getAwsRegion())
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
        return AwsCredentialsOptions.builder()
                .withRegion(awsAuthenticationOptions.getAwsRegion())
                .withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn())
                .withStsExternalId(awsAuthenticationOptions.getAwsStsExternalId())
                .withStsHeaderOverrides(awsAuthenticationOptions.getAwsStsHeaderOverrides())
                .build();
    }
}