package com.amazon.dataprepper.model.plugin;

import com.amazon.dataprepper.model.configuration.PluginSetting;

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
     * @param <T> The type
     * @return A new instance of your plugin, configured
     * @since 1.2
     */
    <T> T loadPlugin(final Class<T> baseClass, final PluginSetting pluginSetting);

    /**
     * Gets the Java class type for a given plugin. This does not instantiate
     * any new plugin objects.
     *
     * @param baseClass The class type that the plugin supports
     * @param pluginName The name of the plugin
     * @param <T> The type
     * @return The Java class type for this plugin
     * @since 1.2
     */
    <T> Class<? extends T> getPluginClass(final Class<T> baseClass, final String pluginName);
}
