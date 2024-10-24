/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import java.util.Collection;
import java.util.Optional;

/**
 * Implementing classes are able to provide plugin classes to the plugin management
 * system.
 *
 * @see ClasspathPluginProvider
 * @since 1.2
 */
public interface PluginProvider {

    /**
     * Finds the Java class for a specific plugin.
     *
     * @param pluginType The type of plugin which is being supported.
     *                   e.g. {@link org.opensearch.dataprepper.model.sink.Sink}.
     * @param pluginName The name of the plugin
     * @param <T> The type
     * @return An {@link Optional} Java class for this plugin
     * @since 1.2
     */
    <T> Optional<Class<? extends T>> findPluginClass(Class<T> pluginType, String pluginName);

    /**
     * Finds the Java classes for a specific pluginType.
     *
     * @param pluginType The type of plugin which is being supported.
     *                   e.g. {@link org.opensearch.dataprepper.model.sink.Sink}.
     * @param <T> The type
     * @return An {@link Collection} of Java classes for plugins
     * @since 1.2
     */
    <T> Collection<Class<? extends T>> findPluginClasses(Class<T> pluginType);
}
