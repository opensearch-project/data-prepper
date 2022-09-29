/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.Map;

/**
 * An interface which is responsible for mapping grok custom attributes
 * from Logstash plugin into Data Prepper plugin models.
 *
 * @since 1.3
 */
interface GrokLogstashPluginAttributeMapperHelper {
    /**
     * Populate all logstash grok attributes if there are multiple.
     *
     * @param logstashAttribute A custom logstash attribute for the grok
     * @since 1.3
     */
    void populateAttributes(LogstashAttribute logstashAttribute);

    /**
     * Update plugin settings
     *
     * @param logstashAttributesMappings The mappings for this Logstash plugin
     * @param pluginSettings The plugin settings for this Logstash plugin used for creating {@link PluginModel}
     * @since 1.3
     */
    void updatePluginSettings(LogstashAttributesMappings logstashAttributesMappings, Map<String, Object> pluginSettings);
}
