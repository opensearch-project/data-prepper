package com.amazon.dataprepper.logstash.model;

import java.util.List;

/**
 * Class to hold Logstash configuration plugin name and list of {@link LogstashAttribute}
 *
 * @since 1.2
 */
public class LogstashPlugin {
    private final String pluginName;
    private final List<LogstashAttribute> attributes;

    public String getPluginName() {
        return pluginName;
    }

    public List<LogstashAttribute> getAttributes() {
        return attributes;
    }

    private LogstashPlugin(LogstashPluginBuilder builder) {
        this.pluginName = builder.pluginName;
        this.attributes = builder.attributes;
    }

    public static class LogstashPluginBuilder {
        private String pluginName;
        private List<LogstashAttribute> attributes;

        public LogstashPluginBuilder pluginName(final String pluginName) {
            this.pluginName = pluginName;
            return this;
        }

        public LogstashPluginBuilder attributes(final List<LogstashAttribute> attributes) {
            this.attributes = attributes;
            return this;
        }

        public LogstashPlugin build() {
            return new LogstashPlugin(this);
        }
    }
}
