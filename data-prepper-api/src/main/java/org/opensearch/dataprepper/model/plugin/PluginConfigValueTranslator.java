/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.model.plugin;

/**
 * Interface for a Plugin configuration value translator.
 * It translates a string expression that is describing a secret store Id and secret Key in to a secretValue
 * extracted from corresponding secret store.
 *
 * @since 2.0
 */

public interface PluginConfigValueTranslator {
    /**
     * Translates a string expression that is describing a secret store Id and secret Key in to a secretValue
     * extracted from corresponding secret store.
     * Example expression:  ${{aws_secrets:secretId:secretKey}}
     *
     * @param value the string value to translate
     * @return the translated object
     */
    Object translate(final String value);

    /**
     * Returns the prefix for this translator.
     *
     * @return the prefix for this translator
     */
    String getPrefix();

    /**
     * Translates a string expression that is describing a secret store Id and secret Key in to an instance
     * of PluginConfigVariable with secretValue extracted from corresponding secret store. Additionally,
     * this PluginConfigVariable helps with updating the secret value in the secret store, if required.
     * Example expression:  ${{aws_secrets:secretId:secretKey}}
     *
     * @param value the string value to translate
     * @return the translated object
     */
    PluginConfigVariable translateToPluginConfigVariable(final String value);
}
