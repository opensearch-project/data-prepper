/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logstash.mapping.GrokLogstashPluginAttributesMapper.LOGSTASH_GROK_OVERWRITE_ATTRIBUTE_NAME;

/**
 * A class which is responsible for mapping grok overwrite attribute
 * from Logstash plugin into Data Prepper plugin models.
 *
 * @since 1.3
 */
class GrokOverwriteAttributeHelper implements GrokLogstashPluginAttributeMapperHelper {
    final List<String> keysToOverwrite = new ArrayList<>();

    @SuppressWarnings("unchecked")
    @Override
    public void populateAttributes(final LogstashAttribute logstashAttribute) {
        keysToOverwrite.addAll((List<String>) logstashAttribute.getAttributeValue().getValue());
    }

    @Override
    public void updatePluginSettings(final LogstashAttributesMappings logstashAttributesMappings, final Map<String, Object> pluginSettings) {
        if (!keysToOverwrite.isEmpty()) {
            pluginSettings.put(
                    logstashAttributesMappings.getMappedAttributeNames().get(LOGSTASH_GROK_OVERWRITE_ATTRIBUTE_NAME),
                    keysToOverwrite.stream().map(NestedSyntaxConverter::convertNestedSyntaxToJsonPointer).collect(Collectors.toList())
            );
        }
    }
}
