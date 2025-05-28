/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.encryption;

@FunctionalInterface
public interface KeyProvider {
    byte[] decryptKey(byte[] encryptedKey);
}
