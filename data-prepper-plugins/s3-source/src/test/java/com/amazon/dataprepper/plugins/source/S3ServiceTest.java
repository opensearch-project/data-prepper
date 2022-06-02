/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3ServiceTest {

    S3SourceConfig s3SourceConfig;
    S3Service s3Service;
    AwsAuthenticationOptions awsAuthenticationOptions;
    StsClient stsClient = mock(StsClient.class);

    @BeforeEach
    void setUp() {
        s3SourceConfig = mock(S3SourceConfig.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);

        when(s3SourceConfig.getAWSAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn("us-east-1");

        s3Service = new S3Service(s3SourceConfig);
    }

    @Test
    void createS3Client_should_return_instance_of_S3Client() {
        assertThat(s3SourceConfig.getAWSAuthenticationOptions().getAwsRegion(), equalTo("us-east-1"));
        assertThat(s3Service.createS3Client(stsClient), instanceOf(S3Client.class));
    }

}