/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import java.util.Objects;

class DefinedPlugin<T> {
    private final Class<? extends T> pluginClass;
    private final String pluginName;

    public DefinedPlugin(final Class<? extends T> pluginClass, final String pluginName) {
        this.pluginClass = Objects.requireNonNull(pluginClass);
        this.pluginName = Objects.requireNonNull(pluginName);
    }

    public Class<? extends T> getPluginClass() {
        return pluginClass;
    }

    public String getPluginName() {
        return pluginName;
    }
}
