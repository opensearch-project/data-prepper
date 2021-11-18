/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.List;
import java.util.Map;
import java.util.HashSet;

class DefaultLogstashPluginAttributesMapper extends AbstractLogstashPluginAttributesMapper {

    @Override
    protected void mapCustomAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings, Map<String, Object> pluginSettings) {
        // No custom attributes to map
    }

    @Override
    protected HashSet<String> getCustomMappedAttributeNames() {
        return new HashSet<>();
    }

}
