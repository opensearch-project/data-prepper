/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import lombok.Builder;
import lombok.Getter;
import org.opensearch.dataprepper.model.encryption.EncryptionEnvelope;

@Builder
@Getter
class DefaultEncryptionEnvelope implements EncryptionEnvelope {
    private byte[] encryptedData;
    private String encryptedDataKey;

    @Override
    public byte[] getEncryptedData() {
        return encryptedData;
    }

    @Override
    public String getEncryptedDataKey() {
        return encryptedDataKey;
    }
}
