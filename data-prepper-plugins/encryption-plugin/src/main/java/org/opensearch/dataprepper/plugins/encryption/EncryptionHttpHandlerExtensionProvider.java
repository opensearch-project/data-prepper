/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

class EncryptionHttpHandlerExtensionProvider implements ExtensionProvider<EncryptionHttpHandler> {
    private final EncryptionHttpHandler encryptionHttpHandler;

    public EncryptionHttpHandlerExtensionProvider(final EncryptionHttpHandler encryptionHttpHandler) {
        this.encryptionHttpHandler = encryptionHttpHandler;
    }

    @Override
    public Optional<EncryptionHttpHandler> provideInstance(Context context) {
        return Optional.ofNullable(encryptionHttpHandler);
    }

    @Override
    public Class<EncryptionHttpHandler> supportedClass() {
        return EncryptionHttpHandler.class;
    }
}
