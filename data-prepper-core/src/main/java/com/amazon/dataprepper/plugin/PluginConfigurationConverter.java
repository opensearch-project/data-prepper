package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import java.util.Objects;

class PluginConfigurationConverter {
    private final ObjectMapper objectMapper;

    PluginConfigurationConverter() {
        this.objectMapper = new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }

    public Object convert(final Class<?> pluginConfigurationType, final PluginSetting pluginSetting) {
        Objects.requireNonNull(pluginConfigurationType);
        Objects.requireNonNull(pluginSetting);

        if(pluginConfigurationType.equals(PluginSetting.class))
            return pluginSetting;

        return objectMapper.convertValue(pluginSetting.getSettings(), pluginConfigurationType);
    }
}
