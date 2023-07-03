/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.aws.sqs.common;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.Map;

/**
 * A Factory for creating and fetching the <code>SqsClient</code>.
 */
public class ClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ClientFactory.class);

    private ClientFactory(){
    }


    /**
     * Create a SqsClient Object for reading the sqs messages
     * @param region - aws region should be required
     * @param roleArn - aws role arn should be required
     * @param stsHeader - aws role header should be required
     * @param awsCredentialsSupplier - aws credentials supplier should be required
     * @return a SqsClient Object
     */
    public static SqsClient createSqsClient(final Region region,
                                     final String roleArn,
                                     final Map<String,String> stsHeader,
                                     final AwsCredentialsSupplier awsCredentialsSupplier) {
        LOG.info("Creating SQS client");
        return SqsClient.builder()
                .region(region)
                .credentialsProvider(awsCredentialsSupplier.getProvider(convertToCredentialOptions(region,roleArn,stsHeader)))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(builder -> builder.numRetries(5).build())
                        .build())
                .build();
    }

    private static AwsCredentialsOptions convertToCredentialOptions(final Region region,
                                                                    final String roleArn,
                                                                    final Map<String,String> stsHeader) {
        return AwsCredentialsOptions.builder()
                .withRegion(region)
                .withStsRoleArn(roleArn)
                .withStsHeaderOverrides(stsHeader)
                .build();
    }
}
