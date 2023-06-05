/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.sqssource;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.source.sqssource.config.AwsAuthenticationOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.sqs.SqsClient;

public class ClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ClientFactory.class);

    private ClientFactory(){
    }

    static SqsClient createSqsClient(final AwsAuthenticationOptions awsAuthenticationOptions,
                              final AwsCredentialsSupplier awsCredentialsSupplier) {
        LOG.info("Creating SQS client");
        return SqsClient.builder()
                .region(awsAuthenticationOptions.getAwsRegion())
                .credentialsProvider(awsCredentialsSupplier.getProvider(convertToCredentialOptions(awsAuthenticationOptions)))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(builder -> builder.numRetries(5).build())
                        .build())
                .build();
    }

    private static AwsCredentialsOptions convertToCredentialOptions(final AwsAuthenticationOptions awsAuthenticationOptions) {
        return AwsCredentialsOptions.builder()
                .withRegion(awsAuthenticationOptions.getAwsRegion())
                .withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn())
                .withStsHeaderOverrides(awsAuthenticationOptions.getAwsStsHeaderOverrides())
                .build();
    }
}
