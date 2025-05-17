/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import software.amazon.awssdk.services.s3.S3Client;

public class EncryptedDataKeyWriterFactory {
    public S3EncryptedDataKeyWriter createS3EncryptedDataKeyWriter(
            final S3Client s3Client, final String encryptionKeyDirectory) {
        return new S3EncryptedDataKeyWriter(s3Client, encryptionKeyDirectory);
    }

    public LocalDirectoryEncryptedDataKeyWriter createLocalDirectoryEncryptedDataKeyWriter(
            final String encryptionKeyDirectory) {
        return new LocalDirectoryEncryptedDataKeyWriter(encryptionKeyDirectory);
    }
}
