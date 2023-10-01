package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginConfigSubscriber;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultPluginConfigObservable implements PluginConfigObservable {
    private final Map<PluginConfigSubscriber, Boolean> pluginConfigSubscriberBooleanMap
            = new ConcurrentHashMap<>();
    private final PluginConfigurationConverter pluginConfigurationConverter;
    private final Class<?> pluginConfigClass;
    private final PluginSetting rawPluginSettings;

    public DefaultPluginConfigObservable(final PluginConfigurationConverter pluginConfigurationConverter,
                                         final Class<?> pluginConfigClass,
                                         final PluginSetting rawPluginSettings) {
        this.pluginConfigurationConverter = pluginConfigurationConverter;
        this.pluginConfigClass = pluginConfigClass;
        this.rawPluginSettings = rawPluginSettings;
    }

    @Override
    public Class<?> pluginConfigClass() {
        return pluginConfigClass;
    }

    @Override
    public PluginSetting rawPluginSettings() {
        return rawPluginSettings;
    }

    @Override
    public Boolean addPluginConfigSubscriber(final PluginConfigSubscriber pluginConfigSubscriber) {
        return pluginConfigSubscriberBooleanMap.put(pluginConfigSubscriber, true);
    }

    @Override
    public void update() {
        final Object newPluginConfiguration = pluginConfigurationConverter.convert(
                pluginConfigClass, rawPluginSettings);
        pluginConfigSubscriberBooleanMap.keySet().forEach(
                pluginConfigSubscriber -> pluginConfigSubscriber.update(newPluginConfiguration));
    }
}
