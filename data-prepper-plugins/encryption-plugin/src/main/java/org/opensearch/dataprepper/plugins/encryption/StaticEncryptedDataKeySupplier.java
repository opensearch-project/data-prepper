/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

class StaticEncryptedDataKeySupplier implements EncryptedDataKeySupplier {
    private final String encryptionKey;

    public StaticEncryptedDataKeySupplier(final String encryptionKey) {
        this.encryptionKey = encryptionKey;
    }

    @Override
    public String retrieveValue() {
        return encryptionKey;
    }

    @Override
    public void refresh() {

    }
}
