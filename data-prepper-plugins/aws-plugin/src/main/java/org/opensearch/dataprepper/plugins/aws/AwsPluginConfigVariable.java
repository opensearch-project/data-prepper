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

import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;

/**
 * AWS Plugin configuration variable implementation.
 */
public class AwsPluginConfigVariable implements PluginConfigVariable {

    private final SecretsSupplier secretsSupplier;
    private final String secretId;
    private final String secretKey;
    private final Object secretValue;

    public AwsPluginConfigVariable(final SecretsSupplier secretsSupplier,
                                   final String secretId, final String secretKey, Object secretValue) {
        this.secretsSupplier = secretsSupplier;
        this.secretId = secretId;
        this.secretKey = secretKey;
        this.secretValue = secretValue;
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public void setValue(Object someValue) {
        this.secretsSupplier.updateValue(secretId, secretKey, someValue);
    }
}
