/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class KeyValueLogstashPluginAttributesMapper extends AbstractLogstashPluginAttributesMapper {
    static final String LOGSTASH_KV_SOURCE_ATTRIBUTE_NAME = "source";
    static final String LOGSTASH_KV_TARGET_ATTRIBUTE_NAME = "target";

    @Override
    protected void mapCustomAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings, Map<String, Object> pluginSettings) {
        Object source = null;
        Object destination = null;
        for (LogstashAttribute logstashAttribute: logstashAttributes) {
            if (logstashAttribute.getAttributeName().equals(LOGSTASH_KV_SOURCE_ATTRIBUTE_NAME)) {
                source = logstashAttribute.getAttributeValue().getValue();
            }
            else if (logstashAttribute.getAttributeName().equals(LOGSTASH_KV_TARGET_ATTRIBUTE_NAME)) {
                destination = logstashAttribute.getAttributeValue().getValue();
            }
        }

        if (source != null) {
            pluginSettings.put(
                    logstashAttributesMappings.getMappedAttributeNames().get(LOGSTASH_KV_SOURCE_ATTRIBUTE_NAME),
                    NestedSyntaxConverterUtil.checkAndConvertLogstashNestedSyntax(source)
            );
        }
        if (destination != null) {
            pluginSettings.put(
                    logstashAttributesMappings.getMappedAttributeNames().get(LOGSTASH_KV_TARGET_ATTRIBUTE_NAME),
                    NestedSyntaxConverterUtil.checkAndConvertLogstashNestedSyntax(destination)
            );
        }
    }

    @Override
    protected HashSet<String> getCustomMappedAttributeNames() {
        return new HashSet<>(Arrays.asList(LOGSTASH_KV_SOURCE_ATTRIBUTE_NAME, LOGSTASH_KV_TARGET_ATTRIBUTE_NAME));
    }
}
