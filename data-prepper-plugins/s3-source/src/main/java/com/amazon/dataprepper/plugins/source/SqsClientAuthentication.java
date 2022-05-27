/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;

public class SqsClientAuthentication {
    private S3SourceConfig s3SourceConfig;
    private AwsAuthenticationOptions awsAuthenticationOptions;

    public SqsClientAuthentication(final S3SourceConfig s3SourceConfig) {
        this.s3SourceConfig = s3SourceConfig;
        awsAuthenticationOptions = s3SourceConfig.getAWSAuthentication();
    }

    public SqsClient createSqsClient(final StsClient stsClient) {

        return SqsClient.builder()
                .credentialsProvider(awsAuthenticationOptions.authenticateAwsConfiguration(stsClient))
                .region(Region.of(s3SourceConfig.getAWSAuthentication().getAwsRegion()))
                .build();
    }

}
