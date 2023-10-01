package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;

import javax.inject.Named;

@Named
public class PluginConfigurationObservableFactory {
    public final PluginConfigObservable createDefaultPluginConfigObservable(
            final PluginConfigurationConverter pluginConfigurationConverter, final Class<?> pluginConfigClass,
            final PluginSetting rawPluginSettings) {
        return new DefaultPluginConfigObservable(
                pluginConfigurationConverter, pluginConfigClass, rawPluginSettings);
    }
}
