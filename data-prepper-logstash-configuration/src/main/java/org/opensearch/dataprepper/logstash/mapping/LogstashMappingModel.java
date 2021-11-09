package org.opensearch.dataprepper.logstash.mapping;

import java.util.Map;

/**
 * Mapping Model class for Logstash plugins and attributes
 *
 * @since 1.2
 */
public class LogstashMappingModel {

    private final String pluginName;
    private final Map<String, Object> mappedAttributes;
    private final Map<String, Object> additionalAttributes;

    public LogstashMappingModel(final String pluginName, final Map<String, Object> mappedAttributes,
                                final Map<String, Object> additionalAttributes) {
        this.pluginName = pluginName;
        this.mappedAttributes = mappedAttributes;
        this.additionalAttributes = additionalAttributes;
    }

    public String getPluginName() {
        return pluginName;
    }

    public Map<String, Object> getMappedAttributes() {
        return mappedAttributes;
    }

    public Map<String, Object> getAdditionalAttributes() {
        return additionalAttributes;
    }
}
