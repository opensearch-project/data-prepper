/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

public class S3ClientAuthentication {
    private S3SourceConfig s3SourceConfig;
    private AwsAuthenticationOptions awsAuthenticationOptions;

    public S3ClientAuthentication(final S3SourceConfig s3SourceConfig) {
        this.s3SourceConfig = s3SourceConfig;
        this.awsAuthenticationOptions = s3SourceConfig.getAWSAuthentication();
    }

    public S3Client createS3Client(final StsClient stsClient) {

        return software.amazon.awssdk.services.s3.S3Client.builder()
                .region(Region.of(s3SourceConfig.getAWSAuthentication().getAwsRegion()))
                .credentialsProvider(awsAuthenticationOptions.authenticateAwsConfiguration(stsClient))
                .build();
    }

}
