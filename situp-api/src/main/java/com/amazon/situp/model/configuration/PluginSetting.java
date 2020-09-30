package com.amazon.situp.model.configuration;

import java.util.Map;

public class PluginSetting {

    private final String name;
    private final Map<String, Object> settings;

    public PluginSetting(final String name, final Map<String, Object> settings) {
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
     * Returns the value of the specified attribute, or null if this settings contains no value for the attribute.
     *
     * @param attribute name of the attribute
     * @return value of the attribute from the metadata
     */
    public Object getAttributeFromSettings(final String attribute) {
        return settings == null ? null : settings.get(attribute);
    }

    /**
     * Returns the value of the specified attribute, or {@code defaultValue} if this settings contains no value for
     * the attribute.
     *
     * @param attribute    name of the attribute
     * @param defaultValue default value for the setting
     * @return the value of the specified attribute, or {@code defaultValue} if this settings contains no value for
     * the attribute
     */
    public Object getAttributeOrDefault(final String attribute, final Object defaultValue) {
        return settings == null ? defaultValue : settings.getOrDefault(attribute, defaultValue);
    }

    /**
     * Returns the value of the specified attribute as integer, or {@code defaultValue} if this settings contains no
     * value for the attribute.
     *
     * @param attribute    name of the attribute
     * @param defaultValue default value for the setting
     * @return the value of the specified attribute, or {@code defaultValue} if this settings contains no value for
     * the attribute
     */
    public Integer getIntegerOrDefault(final String attribute, final int defaultValue) {
        return (Integer) getAttributeOrDefault(attribute, defaultValue);
    }

    /**
     * Returns the value of the specified attribute as String, or {@code defaultValue} if this settings contains no
     * value for the attribute.
     *
     * @param attribute    name of the attribute
     * @param defaultValue default value for the setting
     * @return the value of the specified attribute, or {@code defaultValue} if this settings contains no value for
     * the attribute
     */
    public String getStringOrDefault(final String attribute, final String defaultValue) {
        return (String) getAttributeOrDefault(attribute, defaultValue);
    }

    /**
     * Returns the value of the specified attribute as boolean, or {@code defaultValue} if this settings contains no
     * value for the attribute.
     *
     * @param attribute    name of the attribute
     * @param defaultValue default value for the setting
     * @return the value of the specified attribute, or {@code defaultValue} if this settings contains no value for
     * the attribute
     */
    public Boolean getBooleanOrDefault(final String attribute, final boolean defaultValue) {
        return (Boolean) getAttributeOrDefault(attribute, defaultValue);
    }

    /**
     * Returns the value of the specified attribute as long, or {@code defaultValue} if this settings contains no
     * value for the attribute.
     *
     * @param attribute    name of the attribute
     * @param defaultValue default value for the setting
     * @return the value of the specified attribute, or {@code defaultValue} if this settings contains no value for
     * the attribute
     */
    public Long getLongOrDefault(final String attribute, final long defaultValue) {
        return (Long) getAttributeOrDefault(attribute, defaultValue);
    }

}
