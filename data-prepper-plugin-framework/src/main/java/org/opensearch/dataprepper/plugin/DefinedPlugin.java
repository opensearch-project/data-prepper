/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
