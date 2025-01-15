/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.function.Consumer;

@Named
class DeprecatedPluginDetector implements Consumer<DefinedPlugin<?>> {
    private static final Logger LOG = LoggerFactory.getLogger(DeprecatedPluginDetector.class);

    @Override
    public void accept(final DefinedPlugin<?> definedPlugin) {
        logDeprecatedPluginsNames(definedPlugin.getPluginClass(), definedPlugin.getPluginName());
    }

    private void logDeprecatedPluginsNames(final Class<?> pluginClass, final String pluginName) {
        final String deprecatedName = pluginClass.getAnnotation(DataPrepperPlugin.class).deprecatedName();
        final String name = pluginClass.getAnnotation(DataPrepperPlugin.class).name();
        if (deprecatedName.equals(pluginName)) {
            LOG.warn("Plugin name '{}' is deprecated and will be removed in the next major release. Consider using the updated plugin name '{}'.", deprecatedName, name);
        }
    }
}
