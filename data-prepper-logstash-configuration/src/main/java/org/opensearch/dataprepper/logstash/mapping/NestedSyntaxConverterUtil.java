/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class NestedSyntaxConverterUtil {
    private NestedSyntaxConverterUtil() {}

    public static Object checkAndConvertLogstashNestedSyntax(Object logstashAttributeValue) {
        if (logstashAttributeValue instanceof String && ((String) logstashAttributeValue).startsWith("[") && ((String) logstashAttributeValue).endsWith("]")) {
            return convertAttributeValueNestedSyntax(logstashAttributeValue);
        }
        return logstashAttributeValue;
    }

    private static String convertAttributeValueNestedSyntax(Object attributeValue) {
        StringBuilder convertedAttribute = new StringBuilder();
        String nestedSyntaxRegex = "\\[([^\\]]+)\\]";
        Pattern nestedSyntaxPattern = Pattern.compile(nestedSyntaxRegex);
        Matcher nestedSyntaxMatcher = nestedSyntaxPattern.matcher(attributeValue.toString());

        while (nestedSyntaxMatcher.find()) {
            convertedAttribute.append("/");
            convertedAttribute.append(nestedSyntaxMatcher.group(1));
        }
        return convertedAttribute.toString();
    }
}
