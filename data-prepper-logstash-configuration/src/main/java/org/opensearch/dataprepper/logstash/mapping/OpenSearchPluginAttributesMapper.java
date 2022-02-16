/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.exception.LogstashConfigurationException;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class OpenSearchPluginAttributesMapper extends AbstractLogstashPluginAttributesMapper {
    protected static final String LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME = "index";
    protected static final String REGEX_EXTRACTING_DATETIME_PATTERN = "%\\{(.*?)\\}";

    @Override
    protected void mapCustomAttributes(List<LogstashAttribute> logstashAttributes, LogstashAttributesMappings logstashAttributesMappings, Map<String, Object> pluginSettings) {

        final LogstashAttribute logstashIndexAttribute = logstashAttributes.stream()
                .filter(a -> a.getAttributeName().equals(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME))
                .findFirst()
                .orElseThrow(() -> new LogstashConfigurationException("Index attribute not found"));

        final String convertedIndexPatternValue = convertIndexDateTimePattern(logstashIndexAttribute);

        pluginSettings.put(logstashAttributesMappings.getMappedAttributeNames().get(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME),
                convertedIndexPatternValue);

    }

    @Override
    protected HashSet<String> getCustomMappedAttributeNames() {
        return new HashSet<>(Collections.singletonList(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME));
    }

    private String convertIndexDateTimePattern(LogstashAttribute logstashAttribute) {

        final String logstashIndexAttributeValue = logstashAttribute.getAttributeValue().getValue().toString();
        final Pattern pattern = Pattern.compile(REGEX_EXTRACTING_DATETIME_PATTERN);
        final Matcher dateTimePatternMatcher = pattern.matcher(logstashIndexAttributeValue);

        final StringBuilder updatedDateTimePattern = new StringBuilder();
        String updatedIndexAttributeValue = "";

        if (dateTimePatternMatcher.find()) {
            final String dateTimePattern = dateTimePatternMatcher.group(0);

            for (char character: dateTimePattern.toCharArray()) {
                if (character == '+') {
                    continue;
                } else if (character == 'y') {
                    updatedDateTimePattern.append('u');
                } else if (character == 'Y') {
                    updatedDateTimePattern.append('y');
                } else if (character == 'x') {
                    updatedDateTimePattern.append('Y');
                } else {
                    updatedDateTimePattern.append(character);
                }
            }
            updatedIndexAttributeValue = logstashIndexAttributeValue.replace(dateTimePattern, updatedDateTimePattern);
        }
        return updatedIndexAttributeValue;
    }
}
