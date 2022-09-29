/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.List;

/**
 * An interface for a class which is responsible for mapping attributes
 * from a Logstash plugin into Data Prepper plugin models.
 *
 * @since 1.2
 */
public interface LogstashPluginAttributesMapper {
    /**
     * Map all logstashAttributes from a Logstash plugin.
     *
     * @param logstashAttributes All the Logstash logstashAttributes for the plugin
     * @param logstashAttributesMappings The mappings for this Logstash plugin
     * @return A list of PluginModels
     * @since 1.2
     */
    List<PluginModel> mapAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings);
}
