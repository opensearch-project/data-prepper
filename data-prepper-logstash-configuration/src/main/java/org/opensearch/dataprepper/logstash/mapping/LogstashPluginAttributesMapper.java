/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.List;
import java.util.Map;

/**
 * An interface for a class which is responsible for mapping attributes
 * from a Logstash plugin into Data Prepper plugin settings.
 *
 * @since 1.2
 */
public interface LogstashPluginAttributesMapper {
    /**
     * Map all logstashAttributes from a Logstash plugin.
     *
     * @param logstashAttributes All the Logstash logstashAttributes for the plugin
     * @param logstashAttributesMappings The mappings for this Logstash plugin
     * @return A map of Data Prepper plugin settings.
     * @since 1.2
     */
    Map<String, Object> mapAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings);
}
