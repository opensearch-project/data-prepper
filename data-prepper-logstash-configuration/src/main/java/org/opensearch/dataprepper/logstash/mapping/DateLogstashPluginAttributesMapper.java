/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.exception.LogstashConfigurationException;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;
import org.opensearch.dataprepper.plugins.processor.date.DateProcessorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class DateLogstashPluginAttributesMapper extends AbstractLogstashPluginAttributesMapper {
    private static final Logger LOG = LoggerFactory.getLogger(DateLogstashPluginAttributesMapper.class);
    protected static final String LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME = "match";

    @Override
    protected void mapCustomAttributes(final List<LogstashAttribute> logstashAttributes,
                                       final LogstashAttributesMappings logstashAttributesMappings,
                                       final Map<String, Object> pluginSettings) {

        final LogstashAttribute matchAttribute = logstashAttributes.stream()
                .filter(logstashAttribute -> logstashAttribute.getAttributeName().equals(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME))
                .findFirst()
                .orElseThrow(() -> new LogstashConfigurationException("Missing date match setting in Logstash configuration"));

        final DateProcessorConfig.DateMatch matchAttributeConvertedValue = convertMatchAttribute(matchAttribute);

        pluginSettings.put(
                logstashAttributesMappings.getMappedAttributeNames().get(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME),
                Collections.singletonList(matchAttributeConvertedValue)
        );
    }

    @Override
    protected HashSet<String> getCustomMappedAttributeNames() {
        return new HashSet<>(Collections.singletonList(LOGSTASH_DATE_MATCH_ATTRIBUTE_NAME));
    }

    @SuppressWarnings("unchecked")
    private DateProcessorConfig.DateMatch convertMatchAttribute(final LogstashAttribute matchAttribute) {
        if (matchAttribute.getAttributeValue().getAttributeValueType().equals(LogstashValueType.ARRAY)) {
            final List<String> logstashDateMatchCastValue = (List<String>) matchAttribute.getAttributeValue().getValue();
            if (logstashDateMatchCastValue.size() >= 2) {
                final String logstashAttributeValue = NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(logstashDateMatchCastValue.get(0));
                return new DateProcessorConfig.DateMatch(logstashAttributeValue,
                        logstashDateMatchCastValue.subList(1, logstashDateMatchCastValue.size()));
            }
            else {
                LOG.debug("Logstash date match should have field name and at least one pattern.");
            }
        }
        return null;
    }
}
