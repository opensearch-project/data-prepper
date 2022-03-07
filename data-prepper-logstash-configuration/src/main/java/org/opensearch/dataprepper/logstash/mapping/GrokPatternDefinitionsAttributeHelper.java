/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logstash.mapping.GrokLogstashPluginAttributesMapper.LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME;

/**
 * A class which is responsible for mapping grok pattern definitions attribute
 * from Logstash plugin into Data Prepper plugin models.
 *
 * @since 1.3
 */
class GrokPatternDefinitionsAttributeHelper implements GrokLogstashPluginAttributeMapperHelper {
    private final Map<String, String> patternDefinitions = new HashMap<>();

    @SuppressWarnings("unchecked")
    @Override
    public void populateAttributes(final LogstashAttribute logstashAttribute) {
        patternDefinitions.putAll((Map<String, String>) logstashAttribute.getAttributeValue().getValue());
    }

    @Override
    public void updatePluginSettings(final LogstashAttributesMappings logstashAttributesMappings, final Map<String, Object> pluginSettings) {
        if (!patternDefinitions.isEmpty()) {
            pluginSettings.put(
                    logstashAttributesMappings.getMappedAttributeNames().get(LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME),
                    patternDefinitions
            );
        }
    }

    public void fixNamedCaptures(final Map<String, List<String>> mergedMatch) {
        for (Map.Entry<String, List<String>> entry : mergedMatch.entrySet()) {
            final List<GrokNamedCapturesUtil.GrokNamedCapturesPair> grokNamedCapturesPairs = entry.getValue()
                    .stream().map(GrokNamedCapturesUtil::convertRegexNamedCapturesToGrokPatternDefinitions)
                    .collect(Collectors.toList());
            grokNamedCapturesPairs.forEach(grokNamedCapturesPair ->
                    patternDefinitions.putAll(grokNamedCapturesPair.getMappedPatternDefinitions()));
            final List<String> fixedNameCapturesPatterns = grokNamedCapturesPairs.stream().map(
                    GrokNamedCapturesUtil.GrokNamedCapturesPair::getMappedRegex).collect(Collectors.toList());
            entry.setValue(fixedNameCapturesPatterns);
        }
    }
}
