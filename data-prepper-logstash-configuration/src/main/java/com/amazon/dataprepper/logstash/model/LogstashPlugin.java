package com.amazon.dataprepper.logstash.model;

import java.util.List;

public class LogstashPlugin {
    String pluginName;
    List<LogstashAttribute> attributes;

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
