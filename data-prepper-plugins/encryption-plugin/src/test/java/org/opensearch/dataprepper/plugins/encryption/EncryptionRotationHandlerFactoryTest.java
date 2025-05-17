/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EncryptionRotationHandlerFactoryTest {
    private static final String TEST_ENCRYPTION_ID = UUID.randomUUID().toString();
    private static final String TEST_ENCRYPTION_KEY_DIRECTORY = UUID.randomUUID().toString();

    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private EncryptedDataKeyWriterFactory encryptedDataKeyWriterFactory;
    @Mock
    private KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration;
    @Mock
    private S3EncryptedDataKeyWriter s3EncryptedDataKeyWriter;
    @Mock
    private S3Client s3Client;
    @Mock
    private LocalDirectoryEncryptedDataKeyWriter localDirectoryEncryptedDataKeyWriter;

    private EncryptionRotationHandlerFactory objectUnderTest;

    @Test
    void testCreateKmsEncryptionRotationHandlerWithS3EncryptedDataKeyWriter() {
        when(kmsEncryptionEngineConfiguration.isEncryptionKeyInS3()).thenReturn(true);
        when(kmsEncryptionEngineConfiguration.getEncryptionKeyDirectory()).thenReturn(TEST_ENCRYPTION_KEY_DIRECTORY);
        when(kmsEncryptionEngineConfiguration.createS3Client()).thenReturn(s3Client);
        when(encryptedDataKeyWriterFactory.createS3EncryptedDataKeyWriter(eq(s3Client), eq(TEST_ENCRYPTION_KEY_DIRECTORY)))
                .thenReturn(s3EncryptedDataKeyWriter);
        objectUnderTest = EncryptionRotationHandlerFactory.create(pluginMetrics, encryptedDataKeyWriterFactory);
        final EncryptionRotationHandler encryptionRotationHandler = objectUnderTest.createEncryptionRotationHandler(
                TEST_ENCRYPTION_ID, kmsEncryptionEngineConfiguration);
        assertThat(encryptionRotationHandler, instanceOf(KmsEncryptionRotationHandler.class));
        verify(encryptedDataKeyWriterFactory).createS3EncryptedDataKeyWriter(s3Client, TEST_ENCRYPTION_KEY_DIRECTORY);
    }

    @Test
    void testCreateKmsEncryptionRotationHandlerWithLocalDirectoryEncryptedDataKeyWriter() {
        when(kmsEncryptionEngineConfiguration.isEncryptionKeyInS3()).thenReturn(false);
        when(kmsEncryptionEngineConfiguration.getEncryptionKeyDirectory()).thenReturn(TEST_ENCRYPTION_KEY_DIRECTORY);
        when(encryptedDataKeyWriterFactory.createLocalDirectoryEncryptedDataKeyWriter(eq(TEST_ENCRYPTION_KEY_DIRECTORY)))
                .thenReturn(localDirectoryEncryptedDataKeyWriter);
        objectUnderTest = EncryptionRotationHandlerFactory.create(pluginMetrics, encryptedDataKeyWriterFactory);
        final EncryptionRotationHandler encryptionRotationHandler = objectUnderTest.createEncryptionRotationHandler(
                TEST_ENCRYPTION_ID, kmsEncryptionEngineConfiguration);
        assertThat(encryptionRotationHandler, instanceOf(KmsEncryptionRotationHandler.class));
        verify(encryptedDataKeyWriterFactory).createLocalDirectoryEncryptedDataKeyWriter(TEST_ENCRYPTION_KEY_DIRECTORY);
    }
}