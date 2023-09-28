package org.opensearch.dataprepper.model.plugin;

import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.Map;

public interface PluginConfigurationObservable {
    Class<?> pluginConfigClass();

    PluginSetting rawPluginSettings();

    Boolean addPluginConfigSubscriber(PluginConfigSubscriber pluginConfigSubscriber);

    void update();
}
