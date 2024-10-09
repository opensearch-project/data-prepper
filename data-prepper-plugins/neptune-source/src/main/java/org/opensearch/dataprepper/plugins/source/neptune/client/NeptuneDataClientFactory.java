/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.neptune.client;

import org.opensearch.dataprepper.plugins.source.neptune.configuration.AwsConfig;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.NeptuneSourceConfig;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.neptunedata.NeptunedataClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;
import java.util.UUID;

public class NeptuneDataClientFactory {
    public static NeptunedataClient provideNeptuneDataClient(final NeptuneSourceConfig sourceConfig) {
        final AwsConfig awsConfig = sourceConfig.getAwsConfig();
        return NeptunedataClient
                .builder()
                .region(Region.of(sourceConfig.getRegion()))
                .credentialsProvider(getAwsCredentials(Region.of(sourceConfig.getRegion()), awsConfig.getAwsStsRoleArn(), awsConfig.getAwsStsExternalId()))
                .endpointOverride(URI.create(String.format("https://%s:%s", sourceConfig.getHost(), sourceConfig.getPort()))).build();
    }

    private static AwsCredentialsProvider getAwsCredentials(
            final Region region, final String stsRoleArn, final String stsExternalId
    ) {
        AwsCredentialsProvider awsCredentialsProvider;
        if (stsRoleArn != null && !stsRoleArn.isEmpty()) {
            try {
                Arn.fromString(stsRoleArn);
            } catch (final Exception e) {
                throw new IllegalArgumentException("Invalid ARN format for dynamodb sts_role_arn");
            }

            final StsClient stsClient = StsClient.builder()
                    .region(region)
                    .build();

            AssumeRoleRequest.Builder assumeRoleRequestBuilder = AssumeRoleRequest.builder()
                    .roleSessionName("Dynamo-Source-Coordination-" + UUID.randomUUID())
                    .roleArn(stsRoleArn);

            if (stsExternalId != null && !stsExternalId.isEmpty()) {
                assumeRoleRequestBuilder = assumeRoleRequestBuilder.externalId(stsExternalId);
            }

            awsCredentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(assumeRoleRequestBuilder.build())
                    .build();

        } else {
            // use default credential provider
            awsCredentialsProvider = DefaultCredentialsProvider.create();
        }

        return awsCredentialsProvider;
    }
}
