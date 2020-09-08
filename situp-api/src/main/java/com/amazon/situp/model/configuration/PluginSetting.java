package com.amazon.situp.model.configuration;

import java.util.Map;

public class PluginSetting {

    private final String name;
    private final Map<String, Object> settings;

    public PluginSetting(
            final String name,
            final Map<String, Object> settings) {
        this.name = name;
        this.settings = settings;
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> getSettings() {
        return settings;
    }

    /**
     * Retrieves the value of the provided attribute (if exists), null otherwise.
     * TODO: Add more methods to return specific Strings/integers instead of Objects
     *
     * @param attribute name of the attribute
     * @return value of the attribute from the metadata
     */
    public Object getAttributeFromSettings(final String attribute) {
        return settings.get(attribute);
    }

}
