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
    private final GrokLogstashPluginAttributeMapperHelper matchAttributesHelper;
    private final GrokLogstashPluginAttributeMapperHelper patternDefinitionsAttributeHelper;
    private final GrokLogstashPluginAttributeMapperHelper overwriteAttributeHelper;

    protected static final String LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME = "match";
    protected static final String LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME = "pattern_definitions";
    protected static final String LOGSTASH_GROK_OVERWRITE_ATTRIBUTE_NAME = "overwrite";
    private static final Map<String, GrokLogstashPluginAttributeMapperHelper> HELPER_INSTANCES = new HashMap<>();
    private static  final HashSet<String> customAttributes = new HashSet<>(Arrays.asList(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME,
            LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME,
            LOGSTASH_GROK_OVERWRITE_ATTRIBUTE_NAME));

    public GrokLogstashPluginAttributesMapper() {
        matchAttributesHelper = new GrokMatchAttributeHelper();
        patternDefinitionsAttributeHelper = new GrokPatternDefinitionsAttributeHelper();
        overwriteAttributeHelper = new GrokOverwriteAttributeHelper();

        HELPER_INSTANCES.put(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME, matchAttributesHelper);
        HELPER_INSTANCES.put(LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME, patternDefinitionsAttributeHelper);
        HELPER_INSTANCES.put(LOGSTASH_GROK_OVERWRITE_ATTRIBUTE_NAME, overwriteAttributeHelper);
    }

    @Override
    protected void mapCustomAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings, Map<String, Object> pluginSettings) {
        logstashAttributes.forEach(logstashAttribute -> {
            if (HELPER_INSTANCES.containsKey(logstashAttribute.getAttributeName()))
                HELPER_INSTANCES.get(logstashAttribute.getAttributeName()).populateAttributes(logstashAttribute);
        });

        ((GrokPatternDefinitionsAttributeHelper) patternDefinitionsAttributeHelper).fixNamedCaptures(
                ((GrokMatchAttributeHelper) matchAttributesHelper).mergeMatchAttributes());

        HELPER_INSTANCES.values().forEach(attributeHelper ->
                attributeHelper.updatePluginSettings(logstashAttributesMappings, pluginSettings));
    }

    @Override
    protected HashSet<String> getCustomMappedAttributeNames() {
        return customAttributes;
    }
}
