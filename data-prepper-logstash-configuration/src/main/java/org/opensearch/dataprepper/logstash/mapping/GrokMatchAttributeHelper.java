/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.logstash.mapping.GrokLogstashPluginAttributesMapper.LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME;

/**
 * A class which is responsible for mapping grok match attribute
 * from Logstash plugin into Data Prepper plugin models.
 *
 * @since 1.3
 */
class GrokMatchAttributeHelper implements GrokLogstashPluginAttributeMapperHelper {
    private static final Logger LOG = LoggerFactory.getLogger(GrokMatchAttributeHelper.class);

    private final List<LogstashAttribute> matchAttributes = new ArrayList<>();
    private final Map<String, List<String>> dataPrepperGrokMatch = new HashMap<>();
    @Override
    public void populateAttributes(final LogstashAttribute logstashAttribute) {
        matchAttributes.add(logstashAttribute);
    }

    @Override
    public void updatePluginSettings(final LogstashAttributesMappings logstashAttributesMappings, final Map<String, Object> pluginSettings) {
        if (!dataPrepperGrokMatch.isEmpty()) {
            pluginSettings.put(
                    logstashAttributesMappings.getMappedAttributeNames().get(LOGSTASH_GROK_MATCH_ATTRIBUTE_NAME),
                    dataPrepperGrokMatch
            );
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, List<String>> mergeMatchAttributes() {
        matchAttributes.forEach(matchLogstashAttribute -> {
            final LogstashValueType logstashGrokMatchValueType = matchLogstashAttribute.getAttributeValue().getAttributeValueType();
            final Object logstashGrokMatchValue = matchLogstashAttribute.getAttributeValue().getValue();
            if (logstashGrokMatchValueType.equals(LogstashValueType.HASH)) {
                final Map<String, String> logstashGrokMatchCastValue = (Map<String, String>) logstashGrokMatchValue;
                logstashGrokMatchCastValue.forEach((key, val) -> {
                    final String grokMatchKey = NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(key);
                    if (!dataPrepperGrokMatch.containsKey(grokMatchKey)) {
                        dataPrepperGrokMatch.put(grokMatchKey, new ArrayList<>());
                    }
                    dataPrepperGrokMatch.get(grokMatchKey).add(GrokMatchUtil.convertGrokMatchPattern(val));
                });
            } else if (logstashGrokMatchValueType.equals(LogstashValueType.ARRAY)) {
                final List<String> logstashGrokMatchCastValue = (List<String>) logstashGrokMatchValue;
                if (logstashGrokMatchCastValue.size() == 2) {
                    final String key = logstashGrokMatchCastValue.get(0);
                    final String val = GrokMatchUtil.convertGrokMatchPattern(logstashGrokMatchCastValue.get(1));
                    String grokMatchKey = NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(key);
                    if (!dataPrepperGrokMatch.containsKey(grokMatchKey)) {
                        dataPrepperGrokMatch.put(grokMatchKey, new ArrayList<>());
                    }
                    dataPrepperGrokMatch.get(grokMatchKey).add(val);
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
