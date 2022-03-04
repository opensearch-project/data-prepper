/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

class GrokLogstashPluginAttributesMapper extends AbstractLogstashPluginAttributesMapper {
    protected static final String LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME = "match";
    protected static final String LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME = "pattern_definitions";
    protected static final String LOGSTASH_GROK_OVERWRITE_ATTRIBUTE_NAME = "overwrite";
    private static  final HashSet<String> customAttributes = new HashSet<>(Arrays.asList(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME,
            LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME,
            LOGSTASH_GROK_OVERWRITE_ATTRIBUTE_NAME));

    @Override
    protected void mapCustomAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings, Map<String, Object> pluginSettings) {
        GrokLogstashPluginAttributeMapperHelper matchAttributesHelper = new GrokMatchAttributeHelper();
        GrokLogstashPluginAttributeMapperHelper patternDefinitionsAttributeHelper = new GrokPatternDefinitionsAttributeHelper();
        GrokLogstashPluginAttributeMapperHelper overwriteAttributeHelper = new GrokOverwriteAttributeHelper();

        Map<String, GrokLogstashPluginAttributeMapperHelper> helperInstances = new HashMap<>();
        helperInstances.put(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME, matchAttributesHelper);
        helperInstances.put(LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME, patternDefinitionsAttributeHelper);
        helperInstances.put(LOGSTASH_GROK_OVERWRITE_ATTRIBUTE_NAME, overwriteAttributeHelper);

        logstashAttributes.forEach(logstashAttribute -> {
            if (helperInstances.containsKey(logstashAttribute.getAttributeName()))
                helperInstances.get(logstashAttribute.getAttributeName()).populateAttributes(logstashAttribute);
        });

        ((GrokPatternDefinitionsAttributeHelper) patternDefinitionsAttributeHelper).fixNamedCaptures(
                ((GrokMatchAttributeHelper) matchAttributesHelper).mergeMatchAttributes());

        helperInstances.values().forEach(attributeHelper ->
                attributeHelper.updatePluginSettings(logstashAttributesMappings, pluginSettings));
    }

    @Override
    protected HashSet<String> getCustomMappedAttributeNames() {
        return customAttributes;
    }
}
