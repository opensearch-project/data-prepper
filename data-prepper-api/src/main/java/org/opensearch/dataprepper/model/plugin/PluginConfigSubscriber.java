package org.opensearch.dataprepper.model.plugin;

public interface PluginConfigSubscriber<T> {
    void update(T pluginConfig);
}
