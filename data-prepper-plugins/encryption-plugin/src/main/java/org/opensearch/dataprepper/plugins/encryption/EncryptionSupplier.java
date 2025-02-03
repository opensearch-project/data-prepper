/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.opensearch.dataprepper.model.encryption.EncryptionEngine;

import java.util.Map;
import java.util.stream.Collectors;

public class EncryptionSupplier {
    private final Map<String, EncryptionEngine> encryptionEngineMap;
    private final Map<String, EncryptedDataKeySupplier> encryptedDataKeySupplierMap;
    private final EncryptionEngineFactory encryptionEngineFactory;
    private final EncryptedDataKeySupplierFactory encryptedDataKeySupplierFactory;

    public EncryptionSupplier(final EncryptionPluginConfig encryptionPluginConfig,
                              final EncryptionEngineFactory encryptionEngineFactory,
                              final EncryptedDataKeySupplierFactory encryptedDataKeySupplierFactory) {
        this.encryptionEngineFactory = encryptionEngineFactory;
        this.encryptedDataKeySupplierFactory = encryptedDataKeySupplierFactory;
        encryptedDataKeySupplierMap = toEncryptedDataKeySupplierMap(encryptionPluginConfig);
        encryptionEngineMap = toEncryptionEngineMap(encryptionPluginConfig, encryptedDataKeySupplierMap);
    }

    public EncryptionEngine getEncryptionEngine(final String encryptionId) {
        return encryptionEngineMap.get(encryptionId);
    }

    public EncryptedDataKeySupplier getEncryptedDataKeySupplier(final String encryptionId) {
        return encryptedDataKeySupplierMap.get(encryptionId);
    }

    private Map<String, EncryptedDataKeySupplier> toEncryptedDataKeySupplierMap(
            final EncryptionPluginConfig encryptionPluginConfig) {
        return encryptionPluginConfig.getEncryptionConfigurationMap().entrySet()
                .stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> encryptedDataKeySupplierFactory.createEncryptedDataKeySupplier(entry.getValue()))
                );
    }

    private Map<String, EncryptionEngine> toEncryptionEngineMap(
            final EncryptionPluginConfig encryptionPluginConfig, final Map<String, EncryptedDataKeySupplier> encryptedDataKeySupplierMap) {
        return encryptionPluginConfig.getEncryptionConfigurationMap().entrySet()
                .stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> encryptionEngineFactory.createEncryptionEngine(entry.getValue(), encryptedDataKeySupplierMap.get(entry.getKey()))
                ));
    }
}
