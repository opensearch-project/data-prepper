/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

public interface SecretsSupplier {
    Object retrieveValue(String secretId, String key);

    Object retrieveValue(String secretId);

    void refresh(String secretId);

    /**
     * Update the value of a secret key in the secret store and responds
     * with the version id of the secret after the update.
     *
     * @param secretId      The id of the secret to be updated
     * @param keyToUpdate   The key of the secret to be updated
     * @param newValueToSet The value of the secret to be updated
     * @return The version id of the secret after the update
     */
    String updateValue(String secretId, String keyToUpdate, Object newValueToSet);

    /**
     * Update the value of secret store (which is not a key value secret store) and responds
     * with the version id of the secret after the update.
     *
     * @param secretId      The id of the secret to be updated
     * @param newValueToSet The value of the secret to be updated
     * @return The version id of the secret after the update
     */
    String updateValue(String secretId, Object newValueToSet);
}
