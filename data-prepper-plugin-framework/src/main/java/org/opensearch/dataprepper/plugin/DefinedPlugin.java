/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.plugin.PluginComponentType;

import java.util.Objects;

class DefinedPlugin<T> {
    private final Class<? extends T> pluginClass;
    private final Class<? extends T> pluginTypeClass;
    private final String pluginName;

    public DefinedPlugin(final Class<? extends T> pluginClass,
                         final Class<? extends T> pluginTypeClass,
                         final String pluginName) {
        this.pluginClass = Objects.requireNonNull(pluginClass);
        this.pluginTypeClass = Objects.requireNonNull(pluginTypeClass);
        this.pluginName = Objects.requireNonNull(pluginName);
    }

    public Class<? extends T> getPluginClass() {
        return pluginClass;
    }

    public String getPluginName() {
        return pluginName;
    }

    public String getPluginTypeName() {
        if(pluginTypeClass.isAnnotationPresent(PluginComponentType.class)) {
            return pluginTypeClass.getAnnotation(PluginComponentType.class).value();
        }

        return null;
    }
}
