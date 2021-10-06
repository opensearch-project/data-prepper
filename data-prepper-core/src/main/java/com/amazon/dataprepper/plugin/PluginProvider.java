package com.amazon.dataprepper.plugin;

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
     *                   e.g. {@link com.amazon.dataprepper.model.sink.Sink}.
     * @param pluginName The name of the plugin
     * @param <T> The type
     * @return The Java class for this plugin
     * @since 1.2
     */
    <T> Class<? extends T> findPluginClass(Class<T> pluginType, String pluginName);
}
