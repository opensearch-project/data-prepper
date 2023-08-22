/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.s3;

import org.hamcrest.core.IsInstanceOf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class S3ClientProviderTest {

    @Test
    void test_random_region_and_role_should_create_s3client() {
        final String awsRegion = UUID.randomUUID().toString();
        final String awsStsRoleArn = UUID.randomUUID().toString();
        final String awsExternalId = UUID.randomUUID().toString();

        final S3ClientProvider s3ClientProvider =
            new S3ClientProvider(awsRegion, awsStsRoleArn, awsExternalId);

        final S3Client s3Client = s3ClientProvider.buildS3Client();

        assertThat(s3Client, IsInstanceOf.instanceOf(S3Client.class));
    }

    @Test
    void test_random_region_and_null_role_should_create_s3client_with_DefaultCredentialsProvider() {
        final String awsRegion = UUID.randomUUID().toString();
        final String awsStsRoleArn = null;
        final String externalId = null;

        final S3ClientProvider s3ClientProvider = new S3ClientProvider(awsRegion, awsStsRoleArn, externalId);

        final S3Client s3Client = s3ClientProvider.buildS3Client();

        assertThat(s3Client, IsInstanceOf.instanceOf(S3Client.class));
    }
}