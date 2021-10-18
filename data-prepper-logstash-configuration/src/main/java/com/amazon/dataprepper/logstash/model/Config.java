package com.amazon.dataprepper.logstash.model;

import java.util.List;
import java.util.Map;

public class Config {
    Map<PluginType, List<Plugin>> plugins;

    public List<Plugin> getPlugins(PluginType pluginType) {
        return plugins.get(pluginType);
    }

    public void setPlugins(Map<PluginType, List<Plugin>> plugins) {
        this.plugins = plugins;
    }
}
