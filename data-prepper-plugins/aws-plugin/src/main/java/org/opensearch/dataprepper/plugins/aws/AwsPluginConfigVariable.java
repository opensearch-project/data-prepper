/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.plugin.FailedToUpdatePluginConfigValueException;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;

/**
 * AWS Plugin configuration variable implementation.
 */
public class AwsPluginConfigVariable implements PluginConfigVariable {

    private final SecretsSupplier secretsSupplier;
    private final String secretId;
    private final String secretKey;
    private final boolean isUpdatable;
    private Object secretValue;

    public AwsPluginConfigVariable(final SecretsSupplier secretsSupplier,
                                   final String secretId, final String secretKey, Object secretValue) {
        this.secretsSupplier = secretsSupplier;
        this.secretId = secretId;
        this.secretKey = secretKey;
        this.secretValue = secretValue;
        this.isUpdatable = true;
    }

    @Override
    public Object getValue() {
        return secretValue;
    }

    @Override
    public void setValue(Object newValue) {
        if (!isUpdatable()) {
            throw new FailedToUpdatePluginConfigValueException(
                    String.format("Trying to update a secrets that is not updatable. SecretId: %s SecretKey: %s", this.secretId, this.secretKey));
        }
        this.secretsSupplier.updateValue(secretId, secretKey, newValue);
        this.secretValue = newValue;
    }

    @Override
    public void refresh() {
        secretsSupplier.refresh(secretId);
    }

    public void refreshAndRetrieveValue() {
        secretsSupplier.refresh(secretId);
        this.secretValue = secretsSupplier.retrieveValue(secretId, secretKey);
    }


    @Override
    public boolean isUpdatable() {
        return isUpdatable;
    }
}
