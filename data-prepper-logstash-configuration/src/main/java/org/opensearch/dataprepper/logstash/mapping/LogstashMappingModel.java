/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Mapping Model class for Logstash plugins and attributes
 *
 * @since 1.2
 */
class LogstashMappingModel implements LogstashAttributesMappings {

    @JsonProperty
    private String pluginName;

    @JsonProperty
    private String attributesMapperClass;

    @JsonProperty
    private Map<String, String> mappedAttributeNames;

    @JsonProperty
    private Map<String, Object> additionalAttributes;

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
