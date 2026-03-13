/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.encryption.EncryptionEngine;
import org.opensearch.dataprepper.model.encryption.KeyProvider;

class EncryptionEngineFactory {
    private final KeyProviderFactory keyProviderFactory;
    private final PluginMetrics pluginMetrics;

    public static EncryptionEngineFactory create(final KeyProviderFactory keyProviderFactory, final PluginMetrics pluginMetrics) {
        return new EncryptionEngineFactory(keyProviderFactory, pluginMetrics);
    }

    private EncryptionEngineFactory(final KeyProviderFactory keyProviderFactory, final PluginMetrics pluginMetrics) {
        this.keyProviderFactory = keyProviderFactory;
        this.pluginMetrics = pluginMetrics;
    }

    public EncryptionEngine createEncryptionEngine(final EncryptionEngineConfiguration encryptionEngineConfiguration,
                                                   final EncryptedDataKeySupplier encryptedDataKeySupplier) {
        if (encryptionEngineConfiguration instanceof KmsEncryptionEngineConfiguration) {
            final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration =
                    (KmsEncryptionEngineConfiguration) encryptionEngineConfiguration;
            final KeyProvider keyProvider = keyProviderFactory.createKmsKeyProvider(kmsEncryptionEngineConfiguration, pluginMetrics);
            final EncryptionContext encryptionContext = new EncryptionContext();
            return new DefaultEncryptionEngine(keyProvider, encryptionContext, encryptedDataKeySupplier);
        } else {
            throw new IllegalArgumentException("Unsupported encryption engine configuration.");
        }
    }
}
