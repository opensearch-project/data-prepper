/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;

import java.util.HashMap;
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
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    private Map<String, String> mappedAttributeNames = new HashMap<>();

    @JsonProperty
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    private Map<String, Object> additionalAttributes = new HashMap<>();

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
