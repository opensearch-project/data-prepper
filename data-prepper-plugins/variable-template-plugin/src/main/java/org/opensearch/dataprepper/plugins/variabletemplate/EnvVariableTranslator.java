/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.variabletemplate;

import org.opensearch.dataprepper.model.plugin.PluginConfigValueTranslator;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;

public class EnvVariableTranslator implements PluginConfigValueTranslator {

    static final String PREFIX = "env";

    @Override
    public String getPrefix() {
        return PREFIX;
    }

    @Override
    public Object translate(final String name) {
        final String value = getenv(name);
        if (value == null) {
            throw new IllegalArgumentException(
                    String.format("Environment variable '%s' is not set.", name));
        }
        return value;
    }

    @Override
    public PluginConfigVariable translateToPluginConfigVariable(final String name) {
        return new ImmutablePluginConfigVariable(translate(name));
    }

    String getenv(final String name) {
        return System.getenv(name);
    }
}
