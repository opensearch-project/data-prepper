/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;

class GrokLogstashPluginAttributesMapper extends AbstractLogstashPluginAttributesMapper {
    protected static final String LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME = "match";
    protected static final String LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME = "pattern_definitions";
    private static final Logger LOG = LoggerFactory.getLogger(GrokLogstashPluginAttributesMapper.class);

    @SuppressWarnings("unchecked")
    @Override
    protected void mapCustomAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings, Map<String, Object> pluginSettings) {
        final List<LogstashAttribute> matchAttributes = new ArrayList<>();
        final Map<String, String> patternDefinitions = new HashMap<>();
        logstashAttributes.forEach(logstashAttribute -> {
            if (logstashAttribute.getAttributeName().equals(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME)) {
                matchAttributes.add(logstashAttribute);
            } else if (logstashAttribute.getAttributeName().equals(LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME)) {
                patternDefinitions.putAll((Map<String, String>) logstashAttribute.getAttributeValue().getValue());
            }
        });

        final Map<String, List<String>> matchAttributeConvertedValue = mergeMatchAttributes(matchAttributes);
        fixNamedCaptures(matchAttributeConvertedValue, patternDefinitions);
        if (!matchAttributeConvertedValue.isEmpty()) {
            pluginSettings.put(
                    logstashAttributesMappings.getMappedAttributeNames().get(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME),
                    matchAttributeConvertedValue
            );
        }
        if (!patternDefinitions.isEmpty()) {
            pluginSettings.put(
                    logstashAttributesMappings.getMappedAttributeNames().get(LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME),
                    patternDefinitions
            );
        }
    }

    @Override
    protected HashSet<String> getCustomMappedAttributeNames() {
        return new HashSet<>(Arrays.asList(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME, LOGSTASH_GROK_PATTERN_DEFINITIONS_ATTRIBUTE_NAME));
    }

    @SuppressWarnings("unchecked")
    private Map<String, List<String>> mergeMatchAttributes(final List<LogstashAttribute> matchLogstashAttributes) {
        final Map<String, List<String>> dataPrepperGrokMatch = new HashMap<>();
        matchLogstashAttributes.forEach(matchLogstashAttribute -> {
            final LogstashValueType logstashGrokMatchValueType = matchLogstashAttribute.getAttributeValue().getAttributeValueType();
            final Object logstashGrokMatchValue = matchLogstashAttribute.getAttributeValue().getValue();
            if (logstashGrokMatchValueType.equals(LogstashValueType.HASH)) {
                final Map<String, String> logstashGrokMatchCastValue = (Map<String, String>) logstashGrokMatchValue;
                logstashGrokMatchCastValue.forEach((key, val) -> {
                    if (!dataPrepperGrokMatch.containsKey(key)) {
                        dataPrepperGrokMatch.put(key, new ArrayList<>());
                    }
                    dataPrepperGrokMatch.get(key).add(val);
                });
            } else if (logstashGrokMatchValueType.equals(LogstashValueType.ARRAY)) {
                final List<String> logstashGrokMatchCastValue = (List<String>) logstashGrokMatchValue;
                if (logstashGrokMatchCastValue.size() == 2) {
                    final String key = logstashGrokMatchCastValue.get(0);
                    final String val = logstashGrokMatchCastValue.get(1);
                    if (!dataPrepperGrokMatch.containsKey(key)) {
                        dataPrepperGrokMatch.put(key, new ArrayList<>());
                    }
                    dataPrepperGrokMatch.get(key).add(val);
                } else {
                    LOG.warn("logstash grok filter match attribute array should be of size 2");
                }
            } else {
                LOG.warn("Unsupported logstash grok filter match attribute type");
            }
        });
        return dataPrepperGrokMatch;
    }

    private void fixNamedCaptures(final Map<String, List<String>> match, final Map<String, String> patternDefinitions) {
        for (Map.Entry<String, List<String>> entry : match.entrySet()) {
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
