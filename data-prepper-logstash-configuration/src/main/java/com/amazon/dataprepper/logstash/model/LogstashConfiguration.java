package com.amazon.dataprepper.logstash.model;

import java.util.List;
import java.util.Map;

/**
 * Class to hold Logstash configuration {@link LogstashPluginType} and corresponding list of {@link LogstashPlugin}
 *
 * @since 1.2
 */
public class LogstashConfiguration {
    private final Map<LogstashPluginType, List<LogstashPlugin>> pluginSections;

    public List<LogstashPlugin> getPluginSection(LogstashPluginType pluginType) {
        return pluginSections.get(pluginType);
    }

    private LogstashConfiguration(LogstashConfigurationBuilder builder) {
        this.pluginSections = builder.pluginSections;
    }

    public static class LogstashConfigurationBuilder {
        private Map<LogstashPluginType, List<LogstashPlugin>> pluginSections;

        public LogstashConfigurationBuilder pluginSections(LogstashPluginType pluginType, List<LogstashPlugin> plugins) {
            this.pluginSections.put(pluginType, plugins);
            return this;
        }

        public LogstashConfiguration build() {
            return new LogstashConfiguration(this);
        }
    }
}
