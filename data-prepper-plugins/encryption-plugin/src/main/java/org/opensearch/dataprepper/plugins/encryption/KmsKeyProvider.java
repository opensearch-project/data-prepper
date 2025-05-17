/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.opensearch.dataprepper.model.encryption.KeyProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

import java.util.Base64;

public class KmsKeyProvider implements KeyProvider {
    static final Integer MAXIMUM_CACHED_KEYS = 5;

    private final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration;
    private final KmsClient kmsClient;
    private final Cache<String, byte[]> decryptedKeyCache =
            Caffeine.newBuilder()
                    .maximumSize(MAXIMUM_CACHED_KEYS)
                    .build();

    public KmsKeyProvider(final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration) {
        kmsClient = kmsEncryptionEngineConfiguration.createKmsClient();
        this.kmsEncryptionEngineConfiguration = kmsEncryptionEngineConfiguration;
    }

    @Override
    public byte[] apply(String encryptionKey) {
        return decryptedKeyCache.asMap().computeIfAbsent(encryptionKey, key -> {
            final String kmsKeyId = kmsEncryptionEngineConfiguration.getKeyId();

            final byte[] decodedEncryptionKey = Base64.getDecoder().decode(key);
            final DecryptRequest decryptRequest = DecryptRequest.builder()
                    .keyId(kmsKeyId)
                    .ciphertextBlob(SdkBytes.fromByteArray(decodedEncryptionKey))
                    .encryptionContext(kmsEncryptionEngineConfiguration.getEncryptionContext())
                    .build();
            final DecryptResponse decryptResponse = kmsClient.decrypt(decryptRequest);

            return decryptResponse.plaintext().asByteArray();
        });
    }
}
