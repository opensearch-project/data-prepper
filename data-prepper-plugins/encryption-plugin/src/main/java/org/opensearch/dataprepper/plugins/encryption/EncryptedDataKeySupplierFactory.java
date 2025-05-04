/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import software.amazon.awssdk.services.s3.S3Client;

public class EncryptedDataKeySupplierFactory {
    public static EncryptedDataKeySupplierFactory create() {
        return new EncryptedDataKeySupplierFactory();
    }

    private EncryptedDataKeySupplierFactory() {}

    public EncryptedDataKeySupplier createEncryptedDataKeySupplier(
            final EncryptionEngineConfiguration encryptionEngineConfiguration) {
        if (encryptionEngineConfiguration instanceof KmsEncryptionEngineConfiguration) {
            return createKmsEncryptedDataKeySupplier((KmsEncryptionEngineConfiguration) encryptionEngineConfiguration);
        } else {
            throw new IllegalArgumentException(String.format(
                    "Unsupported encryption engine configuration: %s.", encryptionEngineConfiguration.name()));
        }
    }

    private EncryptedDataKeySupplier createKmsEncryptedDataKeySupplier(
            final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration) {
        if (kmsEncryptionEngineConfiguration.getEncryptionKey() != null) {
            return new StaticEncryptedDataKeySupplier(kmsEncryptionEngineConfiguration.getEncryptionKey());
        } else if (kmsEncryptionEngineConfiguration.isEncryptionKeyInS3()) {
            final S3Client s3Client = kmsEncryptionEngineConfiguration.createS3Client();
            return new S3EncryptedDataKeySupplier(
                    s3Client, kmsEncryptionEngineConfiguration.getEncryptionKeyDirectory());
        } else {
            return new LocalDirectoryEncryptedDataKeySupplier(
                    kmsEncryptionEngineConfiguration.getEncryptionKeyDirectory());
        }
    }
}
