package org.opensearch.dataprepper.model.plugin;

public interface PluginComponentRefresher<PluginComponent, PluginConfig> {
    PluginComponent get();

    void setPluginConfig(PluginConfig pluginConfig);
}
