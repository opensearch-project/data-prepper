package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;
import org.opensearch.dataprepper.model.plugin.PluginConfigurationObservable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AwsSecretsPluginConfigPublisher implements PluginConfigPublisher {
    private final Map<PluginConfigurationObservable, Boolean> pluginConfigurationObservableBooleanMap
            = new ConcurrentHashMap<>();

    @Override
    public Boolean addPluginConfigurationObservable(final PluginConfigurationObservable pluginConfigurationObservable) {
        return pluginConfigurationObservableBooleanMap.put(pluginConfigurationObservable, true);
    }

    @Override
    public void notifyAllConfigurationObservable() {
        pluginConfigurationObservableBooleanMap.keySet().forEach(PluginConfigurationObservable::update);
    }
}
