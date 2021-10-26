package com.amazon.dataprepper.model;

import java.util.Map;

public class PluginModel {

    private final String pluginName;
    private final Map<String, Object> pluginSettings;
    private String pipelineName;

    public PluginModel(final String pluginName, final Map<String, Object> pluginSettings) {
        this.pluginName = pluginName;
        this.pluginSettings = pluginSettings;
    }

    public String getPluginName() {
        return pluginName;
    }

    public Map<String, Object> getPluginSettings() {
        return pluginSettings;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }
}
