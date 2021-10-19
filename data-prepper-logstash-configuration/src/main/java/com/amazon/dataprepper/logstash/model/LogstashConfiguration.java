package com.amazon.dataprepper.logstash.model;

import java.util.List;
import java.util.Map;

public class LogstashConfiguration {
    Map<LogstashPluginType, List<LogstashPlugin>> pluginSections;

    public List<LogstashPlugin> getPluginSection(LogstashPluginType pluginType) {
        return pluginSections.get(pluginType);
    }

    public void setPluginSection(LogstashPluginType pluginType, List<LogstashPlugin> plugins) {
        pluginSections.put(pluginType, plugins);
    }
}
