package com.amazon.dataprepper.logstash.model;

import java.util.List;
import java.util.Map;

/**
 * Class to hold Logstash configuration {@link LogstashPluginType} and corresponding list of {@link LogstashPlugin}
 *
 * @since 1.2
 */
public class LogstashConfiguration {
    private Map<LogstashPluginType, List<LogstashPlugin>> pluginSections;

    public List<LogstashPlugin> getPluginSection(LogstashPluginType pluginType) {
        return pluginSections.get(pluginType);
    }

    public void setPluginSection(LogstashPluginType pluginType, List<LogstashPlugin> plugins) {
        pluginSections.put(pluginType, plugins);
    }
}
