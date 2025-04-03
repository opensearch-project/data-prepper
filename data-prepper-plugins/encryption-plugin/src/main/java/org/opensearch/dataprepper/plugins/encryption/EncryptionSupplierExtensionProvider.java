/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.opensearch.dataprepper.model.plugin.ExtensionProvider;

import java.util.Optional;

public class EncryptionSupplierExtensionProvider implements ExtensionProvider<EncryptionSupplier> {
    private final EncryptionSupplier encryptionSupplier;

    public EncryptionSupplierExtensionProvider(final EncryptionSupplier encryptionSupplier) {
        this.encryptionSupplier = encryptionSupplier;
    }

    @Override
    public Optional<EncryptionSupplier> provideInstance(Context context) {
        return Optional.ofNullable(encryptionSupplier);
    }

    @Override
    public Class<EncryptionSupplier> supportedClass() {
        return EncryptionSupplier.class;
    }
}
