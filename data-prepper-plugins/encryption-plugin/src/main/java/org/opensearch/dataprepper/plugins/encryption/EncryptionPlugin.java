/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPoints;

@DataPrepperExtensionPlugin(modelType = EncryptionPluginConfig.class, rootKeyJsonPath = "/encryption")
public class EncryptionPlugin implements ExtensionPlugin {
    private final EncryptionSupplier encryptionSupplier;

    @DataPrepperPluginConstructor
    public EncryptionPlugin(final EncryptionPluginConfig encryptionPluginConfig) {
        if (encryptionPluginConfig != null) {
            final KeyProviderFactory keyProviderFactory = new KeyProviderFactory();
            final EncryptionEngineFactory encryptionEngineFactory = new EncryptionEngineFactory(keyProviderFactory);
            final EncryptedDataKeySupplierFactory encryptedDataKeySupplierFactory =
                    new EncryptedDataKeySupplierFactory();
            encryptionSupplier = new EncryptionSupplier(
                    encryptionPluginConfig, encryptionEngineFactory, encryptedDataKeySupplierFactory);
        } else {
            encryptionSupplier = null;
        }
    }

    @Override
    public void apply(final ExtensionPoints extensionPoints) {
        extensionPoints.addExtensionProvider(new EncryptionSupplierExtensionProvider(encryptionSupplier));
    }
}
