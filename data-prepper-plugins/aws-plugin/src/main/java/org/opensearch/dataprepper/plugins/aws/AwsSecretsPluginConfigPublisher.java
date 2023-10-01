package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AwsSecretsPluginConfigPublisher implements PluginConfigPublisher {
    private final Map<PluginConfigObservable, Boolean> pluginConfigurationObservableBooleanMap
            = new ConcurrentHashMap<>();

    @Override
    public Boolean addPluginConfigObservable(final PluginConfigObservable pluginConfigObservable) {
        return pluginConfigurationObservableBooleanMap.put(pluginConfigObservable, true);
    }

    @Override
    public void notifyAllPluginConfigObservable() {
        pluginConfigurationObservableBooleanMap.keySet().forEach(PluginConfigObservable::update);
    }
}
