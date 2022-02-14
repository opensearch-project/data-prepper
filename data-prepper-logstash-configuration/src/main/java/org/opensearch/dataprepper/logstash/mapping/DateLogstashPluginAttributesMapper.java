/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class DateLogstashPluginAttributesMapper extends AbstractLogstashPluginAttributesMapper {
    protected static final String LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME = "match";

    @Override
    protected void mapCustomAttributes(List<LogstashAttribute> logstashAttributes,
                                       LogstashAttributesMappings logstashAttributesMappings,
                                       Map<String, Object> pluginSettings) {

        LogstashAttribute matchAttribute = null;
        for (LogstashAttribute attribute: logstashAttributes) {
            if (attribute.getAttributeName().equals(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME)) {
                matchAttribute = attribute;
                break;
            }
        }

        final Map<String, List<String>> matchAttributeConvertedValue = convertMatchAttribute(matchAttribute);

        pluginSettings.put(
                logstashAttributesMappings.getMappedAttributeNames().get(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME),
                matchAttributeConvertedValue
        );
    }

    @Override
    protected HashSet<String> getCustomMappedAttributeNames() {
        return new HashSet<>(Collections.singletonList(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME));
    }

    @SuppressWarnings("unchecked")
    private Map<String, List<String>> convertMatchAttribute(LogstashAttribute matchAttribute) {
        Map<String, List<String>> dataPrepperDateMatch = new HashMap<>();

        final LogstashValueType logstashDateMatchValueType = matchAttribute.getAttributeValue().getAttributeValueType();
        final Object logstashDateMatchValue = matchAttribute.getAttributeValue().getValue();

        if (logstashDateMatchValueType.equals(LogstashValueType.ARRAY)) {
            final List<String> logstashDateMatchCastValue = (List<String>) logstashDateMatchValue;
            if (logstashDateMatchCastValue.size() >= 2) {
                dataPrepperDateMatch.put(logstashDateMatchCastValue.get(0),
                        logstashDateMatchCastValue.subList(1, logstashDateMatchCastValue.size()));
            }
        }
        return dataPrepperDateMatch;
    }
}
