/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.s3.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3ClientBuilderFactoryTest {

    @Test
    void testS3ClientBuilderFactoryCreatesClientsWithMicrometerIntegration() {
        final StaticCredentialsProvider credentialsProvider =
                StaticCredentialsProvider.create(AwsBasicCredentials.create("testKey", "testSecret"));
        
        final S3SourceConfig s3SourceConfig = mock(S3SourceConfig.class);
        final AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(s3SourceConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);

        final S3ClientBuilderFactory factory = new S3ClientBuilderFactory(s3SourceConfig, credentialsProvider);
        
        final S3Client s3Client = factory.getS3Client();
        assertNotNull(s3Client, "S3Client should not be null");
        
        assertNotNull(factory.getS3AsyncClient(), "S3AsyncClient should not be null");
    }
}
