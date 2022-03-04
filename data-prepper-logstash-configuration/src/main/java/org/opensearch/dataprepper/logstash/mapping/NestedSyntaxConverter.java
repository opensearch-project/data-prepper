/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class NestedSyntaxConverter {
    private static final String NESTED_SYNTAX_REGEX = "\\[([^\\]]+)\\]";
    private static final Pattern NESTED_SYNTAX_PATTERN = Pattern.compile(NESTED_SYNTAX_REGEX);
    private NestedSyntaxConverter() {}

    public static Object convertNestedSyntaxToJsonPath(final Object logstashAttributeValue) {
        if (logstashAttributeValue instanceof String && ((String) logstashAttributeValue).startsWith("[") && ((String) logstashAttributeValue).endsWith("]")) {
            return convertNestedSyntax(logstashAttributeValue);
        }
        return logstashAttributeValue;
    }

    private static String convertNestedSyntax(final Object attributeValue) {
        StringBuilder convertedAttribute = new StringBuilder();
        Matcher nestedSyntaxMatcher = NESTED_SYNTAX_PATTERN.matcher(attributeValue.toString());

        while (nestedSyntaxMatcher.find()) {
            convertedAttribute.append("/");
            convertedAttribute.append(nestedSyntaxMatcher.group(1));
        }
        return convertedAttribute.toString();
    }
}
