/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.Map;

public interface GrokLogstashPluginAttributeMapperHelper {
    void populateAttributes(LogstashAttribute logstashAttribute);
    void updatePluginSettings(LogstashAttributesMappings logstashAttributesMappings, Map<String, Object> pluginSettings);
}
