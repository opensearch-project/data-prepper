package com.amazon.dataprepper.logstash.model;

import java.util.List;

public class Plugin {
    String pluginName;
    List<Attribute> attributes;

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<Attribute> attributes) {
        this.attributes = attributes;
    }
}
