/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An abstract class which is responsible for mapping basic attributes
 *
 * @since 1.2
 */
public abstract class AbstractLogstashPluginAttributesMapper implements LogstashPluginAttributesMapper {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractLogstashPluginAttributesMapper.class);

    @Override
    public List<PluginModel> mapAttributes(final List<LogstashAttribute> logstashAttributes, final LogstashAttributesMappings logstashAttributesMappings) {

        Objects.requireNonNull(logstashAttributes);
        Objects.requireNonNull(logstashAttributesMappings);
        Objects.requireNonNull(logstashAttributesMappings.getMappedAttributeNames());
        Objects.requireNonNull(logstashAttributesMappings.getAdditionalAttributes());
        Objects.requireNonNull(logstashAttributesMappings.getDefaultSettings());

        final Map<String, Object> pluginSettings = new LinkedHashMap<>(logstashAttributesMappings.getAdditionalAttributes());
        final Map<String, String> mappedAttributeNames = logstashAttributesMappings.getMappedAttributeNames();
        final Map<String, Object> defaultSettings = logstashAttributesMappings.getDefaultSettings();

        Collection<String> customMappedAttributeNames = getCustomMappedAttributeNames();

        logstashAttributes
                .stream()
                .filter(logstashAttribute -> !customMappedAttributeNames.contains(logstashAttribute.getAttributeName()))
                .forEach(logstashAttribute -> {
                    final String logstashAttributeName = logstashAttribute.getAttributeName();
                    final String dataPrepperAttributeName = mappedAttributeNames.get(logstashAttributeName);

                    if (mappedAttributeNames.containsKey(logstashAttributeName)) {
                        Object logstashAttributeValue = logstashAttribute.getAttributeValue().getValue();
                        if (dataPrepperAttributeName.startsWith("!") && logstashAttribute.getAttributeValue().getValue() instanceof Boolean) {
                            pluginSettings.put(dataPrepperAttributeName.substring(1), !(Boolean) logstashAttributeValue);
                        }
                        else {
                            if (logstashAttributesMappings.getNestedSyntaxAttributeNames().contains(logstashAttributeName)) {
                                logstashAttributeValue = NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(logstashAttributeValue.toString());
                            }
                            pluginSettings.put(dataPrepperAttributeName, logstashAttributeValue);
                        }
                    }
                    else {
                        LOG.warn("Logstash Attribute {} is not supported in Data Prepper.", logstashAttributeName);
                    }
                });

        for(Map.Entry<String, Object> defaultSetting: defaultSettings.entrySet()) {
            if(!pluginSettings.containsKey(defaultSetting.getKey())) {
                pluginSettings.put(defaultSetting.getKey(), defaultSetting.getValue());
            }
        }

        if (!customMappedAttributeNames.isEmpty()) {
            mapCustomAttributes(logstashAttributes, logstashAttributesMappings, pluginSettings);
        }

        List<PluginModel> pluginModels = new LinkedList<>();
        pluginModels.add(new PluginModel(logstashAttributesMappings.getPluginName(), pluginSettings));

        return pluginModels;
    }

    /**
     * Map custom logstashAttributes from a Logstash plugin.
     *
     * @param logstashAttributes         All the Logstash logstashAttributes for the plugin
     * @param logstashAttributesMappings The mappings for this Logstash plugin
     * @param pluginSettings             A map of Data Prepper basic plugin settings.
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
