/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class GrokLogstashPluginAttributesMapper implements LogstashPluginAttributesMapper {
    protected static final String LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME = "match";
    private static final Logger LOG = LoggerFactory.getLogger(GrokLogstashPluginAttributesMapper.class);

    @Override
    public Map<String, Object> mapAttributes(final List<LogstashAttribute> logstashAttributes, final LogstashAttributesMappings logstashAttributesMappings) {
        final Map<String, Object> pluginSettings = new LinkedHashMap<>(logstashAttributesMappings.getAdditionalAttributes());

        final List<LogstashAttribute> matchAttributes = new ArrayList<>();
        logstashAttributes.forEach(logstashAttribute -> {
            if (logstashAttribute.getAttributeName().equals(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME)) {
                matchAttributes.add(logstashAttribute);
            } else if (logstashAttributesMappings.getMappedAttributeNames().containsKey(logstashAttribute.getAttributeName())) {
                pluginSettings.put(
                        logstashAttributesMappings.getMappedAttributeNames().get(logstashAttribute.getAttributeName()),
                        logstashAttribute.getAttributeValue().getValue()
                );
            }
            else {
                LOG.warn("Attribute name {} is not found in mapping file.", logstashAttribute.getAttributeName());
            }
        });

        final Map<String, List<String>> matchAttributeConvertedValue = mergeMatchAttributes(matchAttributes);
        if (!matchAttributeConvertedValue.isEmpty()) {
            pluginSettings.put(
                    logstashAttributesMappings.getMappedAttributeNames().get(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME),
                    matchAttributeConvertedValue
            );
        }

        // TODO: fix named capture in the values of matchAttributeConvertedValue and populate pattern_definitions

        return pluginSettings;
    }

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
}
