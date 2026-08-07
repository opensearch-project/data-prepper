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

class KeyProviderFactory {

    public static KeyProviderFactory create() {
        return new KeyProviderFactory();
    }

    private KeyProviderFactory() {}

    public KmsKeyProvider createKmsKeyProvider(
            final KmsEncryptionEngineConfiguration kmsEncryptionEngineConfiguration,
            final PluginMetrics pluginMetrics) {
        return new KmsKeyProvider(kmsEncryptionEngineConfiguration, pluginMetrics);
    }
}
