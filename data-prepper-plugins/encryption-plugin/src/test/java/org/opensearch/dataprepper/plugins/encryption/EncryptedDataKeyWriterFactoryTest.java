/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class EncryptedDataKeyWriterFactoryTest {
    private static final String TEST_ENCRYPTION_KEY_DIRECTORY = UUID.randomUUID().toString();

    private S3Client s3Client;

    private final EncryptedDataKeyWriterFactory objectUnderTest = new EncryptedDataKeyWriterFactory();

    @Test
    void testCreateS3EncryptedDataKeyWriter() {
        assertThat(
                objectUnderTest.createS3EncryptedDataKeyWriter(
                        s3Client, "s3://" + TEST_ENCRYPTION_KEY_DIRECTORY),
                instanceOf(S3EncryptedDataKeyWriter.class));
    }

    @Test
    void testCreateLocalDirectoryEncryptedDataKeyWriter() {
        assertThat(objectUnderTest.createLocalDirectoryEncryptedDataKeyWriter(TEST_ENCRYPTION_KEY_DIRECTORY),
                instanceOf(LocalDirectoryEncryptedDataKeyWriter.class));
    }
}