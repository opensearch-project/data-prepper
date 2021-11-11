package org.opensearch.dataprepper.logstash.mapping;

import java.util.Map;

/**
 * Mapping Model class for Logstash plugins and attributes
 *
 * @since 1.2
 */
class LogstashMappingModel implements LogstashAttributesMappings {

    private String pluginName;
    private String attributesMapperClass;
    private Map<String, String> mappedAttributeNames;
    private Map<String, Object> additionalAttributes;

    public LogstashMappingModel(final String pluginName,
                                final String attributesMapperClass,
                                final Map<String, String> mappedAttributeNames,
                                final Map<String, Object> additionalAttributes) {
        this.pluginName = pluginName;
        this.attributesMapperClass = attributesMapperClass;
        this.mappedAttributeNames = mappedAttributeNames;
        this.additionalAttributes = additionalAttributes;
    }

    public LogstashMappingModel() {}

    public String getPluginName() {
        return pluginName;
    }

    public String getAttributesMapperClass() {
        return attributesMapperClass;
    }

    public Map<String, String> getMappedAttributeNames() {
        return mappedAttributeNames;
    }

    public Map<String, Object> getAdditionalAttributes() {
        return additionalAttributes;
    }
}
