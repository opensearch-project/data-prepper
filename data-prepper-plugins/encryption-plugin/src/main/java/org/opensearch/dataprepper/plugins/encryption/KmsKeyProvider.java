/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.encryption;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.encryption.KeyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;

class KmsKeyProvider implements KeyProvider {
    private static final Logger LOG = LoggerFactory.getLogger(KmsKeyProvider.class);
    private static final String REQUESTS_SUCCEEDED_METRIC_NAME = "kmsRequestsSucceeded";
    private static final String REQUESTS_FAILED_METRIC_NAME = "kmsRequestsFailed";

    static final Integer MAXIMUM_CACHED_KEYS = 5;

    private final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration;
    private final KmsClient kmsClient;
    private final Cache<SdkBytes, byte[]> decryptedKeyCache =
            Caffeine.newBuilder()
                    .maximumSize(MAXIMUM_CACHED_KEYS)
                    .build();
    private final Counter requestsSucceeded;
    private final Counter requestsFailed;

    public KmsKeyProvider(final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration,
                          final PluginMetrics pluginMetrics) {
        kmsClient = kmsEncryptionEngineConfiguration.createKmsClient();
        this.kmsEncryptionEngineConfiguration = kmsEncryptionEngineConfiguration;
        pluginMetrics.gauge("kmsDecryptedKeys", decryptedKeyCache, Cache::estimatedSize);
        requestsSucceeded = pluginMetrics.counter(REQUESTS_SUCCEEDED_METRIC_NAME);
        requestsFailed = pluginMetrics.counter(REQUESTS_FAILED_METRIC_NAME);
    }

    @Override
    public byte[] decryptKey(final byte[] encryptionKey) {
        final SdkBytes encryptionKeyCacheable = SdkBytes.fromByteArray(encryptionKey);
        return decryptedKeyCache.asMap().computeIfAbsent(encryptionKeyCacheable, key -> {
            final String kmsKeyId = kmsEncryptionEngineConfiguration.getKeyId();
            final DecryptRequest decryptRequest = DecryptRequest.builder()
                    .keyId(kmsKeyId)
                    .ciphertextBlob(key)
                    .encryptionContext(kmsEncryptionEngineConfiguration.getEncryptionContext())
                    .build();
            LOG.debug("Calling KMS decrypt for keyId={}", kmsKeyId);
            final DecryptResponse decryptResponse;
            try {
                decryptResponse = kmsClient.decrypt(decryptRequest);
            } catch (final Exception ex) {
                requestsFailed.increment();
                throw ex;
            }
            requestsSucceeded.increment();

            return decryptResponse.plaintext().asByteArray();
        });
    }
}
