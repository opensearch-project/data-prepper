package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginConfigurationObservable;

import javax.inject.Named;

@Named
public class PluginConfigurationObservableFactory {
    public final PluginConfigurationObservable createDefaultPluginConfigurationObservable(
            final PluginConfigurationConverter pluginConfigurationConverter, final Class<?> pluginConfigClass,
            final PluginSetting rawPluginSettings) {
        return new DefaultPluginConfigurationObservable(
                pluginConfigurationConverter, pluginConfigClass, rawPluginSettings);
    }
}
