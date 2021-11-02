package org.opensearch.dataprepper.logstash.model;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

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

    private LogstashPlugin(Builder builder) {
        checkNotNull(builder.pluginName, "plugin name cannot be null");
        checkNotNull(builder.attributes, "attributes cannot be null");

        this.pluginName = builder.pluginName;
        this.attributes = builder.attributes;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A Builder for creating {@link LogstashPlugin} instances.
     *
     * @since 1.2
     */
    public static class Builder {
        private String pluginName;
        private List<LogstashAttribute> attributes;

        public Builder pluginName(final String pluginName) {
            this.pluginName = pluginName;
            return this;
        }

        public Builder attributes(final List<LogstashAttribute> attributes) {
            this.attributes = attributes;
            return this;
        }

        public LogstashPlugin build() {
            return new LogstashPlugin(this);
        }
    }
}
