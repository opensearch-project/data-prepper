/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class DefaultLogstashPluginAttributesMapper implements LogstashPluginAttributesMapper {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultLogstashPluginAttributesMapper.class);

    @Override
    public Map<String, Object> mapAttributes(final List<LogstashAttribute> logstashAttributes, final LogstashAttributesMappings logstashAttributesMappings) {

        Objects.requireNonNull(logstashAttributes);
        Objects.requireNonNull(logstashAttributesMappings);
        Objects.requireNonNull(logstashAttributesMappings.getMappedAttributeNames());
        Objects.requireNonNull(logstashAttributesMappings.getAdditionalAttributes());

        final Map<String, Object> pluginSettings = new LinkedHashMap<>(logstashAttributesMappings.getAdditionalAttributes());

        final Map<String, String> mappedAttributeNames = logstashAttributesMappings.getMappedAttributeNames();
        logstashAttributes.forEach(logstashAttribute -> {
            if (mappedAttributeNames.containsKey(logstashAttribute.getAttributeName())) {
                pluginSettings.put(
                        mappedAttributeNames.get(logstashAttribute.getAttributeName()),
                        logstashAttribute.getAttributeValue().getValue()
                );
            }
            else {
                LOG.warn("Attribute name {} is not found in mapping file.", logstashAttribute.getAttributeName());
            }
        });

        return pluginSettings;
    }
}
