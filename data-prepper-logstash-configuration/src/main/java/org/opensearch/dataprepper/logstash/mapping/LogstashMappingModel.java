package org.opensearch.dataprepper.logstash.mapping;

import java.util.Map;

/**
 * Mapping Model class for Logstash plugins and attributes
 *
 * @since 1.2
 */
public class LogstashMappingModel {

    private String pluginName;
    private Map<String, String> mappedAttributeNames;
    private Map<String, Object> additionalAttributes;

    public LogstashMappingModel(final String pluginName, final Map<String, String> mappedAttributeNames,
                                final Map<String, Object> additionalAttributes) {
        this.pluginName = pluginName;
        this.mappedAttributeNames = mappedAttributeNames;
        this.additionalAttributes = additionalAttributes;
    }

    public LogstashMappingModel() {}

    public String getPluginName() {
        return pluginName;
    }

    public Map<String, String> getMappedAttributeNames() {
        return mappedAttributeNames;
    }

    public Map<String, Object> getAdditionalAttributes() {
        return additionalAttributes;
    }
}
