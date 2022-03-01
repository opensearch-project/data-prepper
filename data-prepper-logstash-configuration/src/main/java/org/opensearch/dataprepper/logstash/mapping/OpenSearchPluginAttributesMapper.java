/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class OpenSearchPluginAttributesMapper extends AbstractLogstashPluginAttributesMapper {

    private static final Map<Character, Character> PATTERN_CONVERTER_MAP;
    static {
        PATTERN_CONVERTER_MAP = new HashMap<>();
        PATTERN_CONVERTER_MAP.put('y', 'u');
        PATTERN_CONVERTER_MAP.put('Y', 'y');
        PATTERN_CONVERTER_MAP.put('x', 'Y');
    }
    protected static final String LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME = "index";
    protected static final String REGEX_EXTRACTING_DATETIME_PATTERN = "%\\{([+].*?)\\}";

    @Override
    protected void mapCustomAttributes(final List<LogstashAttribute> logstashAttributes, final LogstashAttributesMappings logstashAttributesMappings, final Map<String, Object> pluginSettings) {

        final Optional<String> convertedIndexAttributeValue = logstashAttributes.stream()
                .filter(a -> a.getAttributeName().equals(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME))
                .map(this::findAndMatchDateTimePattern)
                .findFirst();

        convertedIndexAttributeValue.ifPresent(s -> pluginSettings.put(logstashAttributesMappings.getMappedAttributeNames().get(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME), s));
    }

    @Override
    protected HashSet<String> getCustomMappedAttributeNames() {
        return new HashSet<>(Collections.singletonList(LOGSTASH_OPENSEARCH_INDEX_ATTRIBUTE_NAME));
    }

    private String findAndMatchDateTimePattern(LogstashAttribute logstashAttribute) {

        final String logstashIndexAttributeValue = logstashAttribute.getAttributeValue().getValue().toString();
        final Pattern dateTimeRegexPattern = Pattern.compile(REGEX_EXTRACTING_DATETIME_PATTERN);
        final Matcher dateTimePatternMatcher = dateTimeRegexPattern.matcher(logstashIndexAttributeValue);

        if (dateTimePatternMatcher.find()) {
            final String dateTimePattern = dateTimePatternMatcher.group(0);
            return logstashIndexAttributeValue.replace(dateTimePattern, convertDateTimePattern(dateTimePattern));
        }
        return logstashIndexAttributeValue;
    }

    private String convertDateTimePattern(final String dateTimePattern) {

        final StringBuilder updatedDateTimePattern = new StringBuilder();
        for (final char character : dateTimePattern.toCharArray()) {
            if (character != '+') {
                final char converterChar = PATTERN_CONVERTER_MAP.getOrDefault(character, character);
                updatedDateTimePattern.append(converterChar);
            }
        }
        return updatedDateTimePattern.toString();
    }
}
