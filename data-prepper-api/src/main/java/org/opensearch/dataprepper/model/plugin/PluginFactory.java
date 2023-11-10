/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.plugin;

import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.List;
import java.util.function.Function;

/**
 * Factory for loading Data Prepper plugins.
 *
 * @since 1.2
 */
public interface PluginFactory {
    /**
     * Loads a new instance of a plugin.
     *
     * @param baseClass The class type that the plugin is supporting.
     * @param pluginSetting The {@link PluginSetting} to configure this plugin
     * @param args variable number of arguments
     * @param <T> The type
     * @return A new instance of your plugin, configured
     * @since 1.2
     */
    <T> T loadPlugin(final Class<T> baseClass, final PluginSetting pluginSetting, final Object ... args);

    /**
    /**
     * Loads a new instance of a plugin with SinkContext.
     *
     * @param baseClass The class type that the plugin is supporting.
     * @param pluginSetting The {@link PluginSetting} to configure this plugin
     * @param sinkContext The {@link SinkContext} to configure this plugin
     * @param <T> The type
     * @return A new instance of your plugin, configured
     * @since 1.2
     */
    <T> T loadPlugin(final Class<T> baseClass, final PluginSetting pluginSetting, final SinkContext sinkContext);

    /**
     * Loads a specified number of plugin instances. The total number of instances is provided
     * by the numberOfInstancesFunction.
     *
     * @param baseClass The class type that the plugin is supporting.
     * @param pluginSetting The {@link PluginSetting} to configure this plugin
     * @param numberOfInstancesFunction A {@link Function} which takes as input a {@link Class}
     *                                  and returns an {@link Integer}. The input class is the actual
     *                                  class plugin class instance. The returned integer value is
     *                                  the total count of instances to create.
     * @param <T> The type
     * @return One or more new instances of the plugin
     * @since 1.2
     */
    <T> List<T> loadPlugins(
            final Class<T> baseClass, final PluginSetting pluginSetting,
            final Function<Class<? extends T>, Integer> numberOfInstancesFunction);
}
