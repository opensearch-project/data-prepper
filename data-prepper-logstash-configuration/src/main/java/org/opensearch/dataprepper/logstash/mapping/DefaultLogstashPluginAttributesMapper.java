/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class DefaultLogstashPluginAttributesMapper extends AbstractLogstashPluginAttributesMapper {

    @Override
    void mapCustomAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings, Map<String, Object> pluginSettings) {
        // No custom attributes to map
    }

    @Override
    Collection<String> getCustomMappedAttributeNames() {
        return Collections.emptyList();
    }


}
