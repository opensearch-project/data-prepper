package org.opensearch.dataprepper.logstash.model;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

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

    private LogstashConfiguration(Builder builder) {
        checkNotNull(builder.pluginSections, "plugin sections cannot be null");

        this.pluginSections = builder.pluginSections;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A Builder for creating {@link LogstashConfiguration} instances.
     *
     * @since 1.2
     */
    public static class Builder {
        private Map<LogstashPluginType, List<LogstashPlugin>> pluginSections;

        public Builder pluginSections(final Map<LogstashPluginType, List<LogstashPlugin>> pluginSections) {
            this.pluginSections = pluginSections;
            return this;
        }

        public LogstashConfiguration build() {
            return new LogstashConfiguration(this);
        }
    }
}
