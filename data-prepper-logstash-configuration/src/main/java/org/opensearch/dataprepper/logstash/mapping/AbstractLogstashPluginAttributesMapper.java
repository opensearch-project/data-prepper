/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.HashSet;

/**
 * An abstract class which is responsible for mapping basic attributes
 *
 * @since 1.2
 */
public abstract class AbstractLogstashPluginAttributesMapper implements LogstashPluginAttributesMapper {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractLogstashPluginAttributesMapper.class);

    @Override
    public Map<String, Object> mapAttributes(final List<LogstashAttribute> logstashAttributes, final LogstashAttributesMappings logstashAttributesMappings) {

        Objects.requireNonNull(logstashAttributes);
        Objects.requireNonNull(logstashAttributesMappings);
        Objects.requireNonNull(logstashAttributesMappings.getMappedAttributeNames());
        Objects.requireNonNull(logstashAttributesMappings.getAdditionalAttributes());

        final Map<String, Object> pluginSettings = new LinkedHashMap<>(logstashAttributesMappings.getAdditionalAttributes());
        final Map<String, String> mappedAttributeNames = logstashAttributesMappings.getMappedAttributeNames();

        Collection<String> customMappedAttributeNames = getCustomMappedAttributeNames();

        logstashAttributes
                .stream()
                .filter(logstashAttribute -> !customMappedAttributeNames.contains(logstashAttribute.getAttributeName()))
                .forEach(logstashAttribute -> {
                    final String logstashAttributeName = logstashAttribute.getAttributeName();
                    final String dataPrepperAttributeName = mappedAttributeNames.get(logstashAttributeName);

                    if (mappedAttributeNames.containsKey(logstashAttributeName)) {
                        if (dataPrepperAttributeName.startsWith("!") && logstashAttribute.getAttributeValue().getValue() instanceof Boolean) {
                            pluginSettings.put(
                                    dataPrepperAttributeName.substring(1), !(Boolean) logstashAttribute.getAttributeValue().getValue()
                            );
                        }
                        else {
                            pluginSettings.put(dataPrepperAttributeName, logstashAttribute.getAttributeValue().getValue());
                        }
                    }
                    else {
                        LOG.warn("Attribute name {} is not found in mapping file.", logstashAttributeName);
                    }
        });

        if (!customMappedAttributeNames.isEmpty()) {
            mapCustomAttributes(logstashAttributes, logstashAttributesMappings, pluginSettings);
        }

        return pluginSettings;
    }

    /**
     * Map custom logstashAttributes from a Logstash plugin.
     *
     * @param logstashAttributes All the Logstash logstashAttributes for the plugin
     * @param logstashAttributesMappings The mappings for this Logstash plugin
     * @param pluginSettings A map of Data Prepper basic plugin settings.
     *
     * @since 1.2
     */
    protected abstract void mapCustomAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings, Map<String, Object> pluginSettings);

    /**
     * Get custom logstashAttributes names from a Logstash plugin.
     *
     * @return A set of custom attributes
     * @since 1.2
     */
    protected abstract HashSet<String> getCustomMappedAttributeNames();
}
