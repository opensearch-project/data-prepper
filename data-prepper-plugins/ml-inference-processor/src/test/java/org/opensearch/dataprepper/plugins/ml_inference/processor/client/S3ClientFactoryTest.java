/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class S3ClientFactoryTest {
    @Mock
    private MLProcessorConfig mlProcessorConfig;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;


    @BeforeEach
    void setUp() {
        when(mlProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_WEST_2);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);
    }

    @Test
    void testCreateS3Client() {
        S3Client s3Client = S3ClientFactory.createS3Client(mlProcessorConfig, awsCredentialsSupplier);
        assertNotNull(s3Client);
        assertEquals(Region.US_WEST_2, mlProcessorConfig.getAwsAuthenticationOptions().getAwsRegion());
    }

    @Test
    void testConvertToCredentialsOptions_WithSTSRole() {
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn("arn:aws:iam::123456789012:role/test-role");
        when(awsAuthenticationOptions.getAwsStsExternalId()).thenReturn("external-id");

        AwsCredentialsOptions credentialsOptions = S3ClientFactory.convertToCredentialsOptions(awsAuthenticationOptions);

        assertNotNull(credentialsOptions);
        assertEquals(Region.US_WEST_2, credentialsOptions.getRegion());
        assertEquals("arn:aws:iam::123456789012:role/test-role", credentialsOptions.getStsRoleArn());
    }

    @Test
    void testConvertToCredentialsOptions_WithDefaultCredentials() {
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(null);

        AwsCredentialsOptions credentialsOptions = S3ClientFactory.convertToCredentialsOptions(awsAuthenticationOptions);

        assertNotNull(credentialsOptions);
    }

}
