package org.opensearch.dataprepper.model.plugin;

public interface PluginConfigPublisher {
    Boolean addPluginConfigurationObservable(PluginConfigurationObservable pluginConfigurationObservable);

    void notifyAllPluginConfigurationObservable();
}
