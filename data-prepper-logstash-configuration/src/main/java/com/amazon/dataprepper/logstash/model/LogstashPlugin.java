package com.amazon.dataprepper.logstash.model;

import java.util.List;

/**
 * Class to hold Logstash configuration plugin name and list of {@link LogstashAttribute}
 *
 * @since 1.2
 */
public class LogstashPlugin {
    private String pluginName;
    private List<LogstashAttribute> attributes;

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }

    public List<LogstashAttribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<LogstashAttribute> attributes) {
        this.attributes = attributes;
    }
}
