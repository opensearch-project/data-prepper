/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import software.amazon.awssdk.services.s3.S3Client;

public class EncryptionRotationHandlerFactory {
    private final PluginMetrics pluginMetrics;
    private final EncryptedDataKeyWriterFactory encryptedDataKeyWriterFactory;

    public EncryptionRotationHandlerFactory(final PluginMetrics pluginMetrics,
                                            final EncryptedDataKeyWriterFactory encryptedDataKeyWriterFactory) {
        this.pluginMetrics = pluginMetrics;
        this.encryptedDataKeyWriterFactory = encryptedDataKeyWriterFactory;
    }

    public EncryptionRotationHandler createEncryptionRotationHandler(
            final String encryptionId,
            final EncryptionEngineConfiguration encryptionEngineConfiguration) {
        if (encryptionEngineConfiguration instanceof KmsEncryptionEngineConfiguration) {
            return createKmsEncryptionRotationHandler(
                    encryptionId,
                    (KmsEncryptionEngineConfiguration) encryptionEngineConfiguration);
        } else {
            throw new IllegalArgumentException("Unsupported.");
        }
    }

    private KmsEncryptionRotationHandler createKmsEncryptionRotationHandler(
            final String encryptionId,
            final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration) {
        if (kmsEncryptionEngineConfiguration.isEncryptionKeyInS3()) {
            final S3Client s3Client = kmsEncryptionEngineConfiguration.createS3Client();
            final EncryptedDataKeyWriter encryptedDataKeyWriter = encryptedDataKeyWriterFactory
                    .createS3EncryptedDataKeyWriter(
                            s3Client, kmsEncryptionEngineConfiguration.getEncryptionKeyDirectory());
            return new KmsEncryptionRotationHandler(
                    encryptionId, kmsEncryptionEngineConfiguration, encryptedDataKeyWriter, pluginMetrics);
        } else {
            throw new IllegalArgumentException("Missing proper destination for writing rotated encryption key.");
        }
    }
}
