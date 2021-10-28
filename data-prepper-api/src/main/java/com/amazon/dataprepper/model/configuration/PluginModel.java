package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;

/**
 * Model class for a Plugin in Configuration YAML containing name of the Plugin and its associated settings
 *
 * @since 1.2
 */
@JsonSerialize(using = PluginModelSerializer.class)
@JsonDeserialize(using = PluginModelDeserializer.class)
public class PluginModel {

    private final String pluginName;
    private final Map<String, Object> pluginSettings;

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
}
