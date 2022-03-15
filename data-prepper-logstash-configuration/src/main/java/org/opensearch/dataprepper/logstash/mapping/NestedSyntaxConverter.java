/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NestedSyntaxConverter {
    private static final String NESTED_SYNTAX_REGEX = "(\\[([^\\]\\[]+)\\])+";
    private static final Pattern NESTED_SYNTAX_PATTERN = Pattern.compile(NESTED_SYNTAX_REGEX);
    private NestedSyntaxConverter() {}

    public static String convertNestedSyntaxToJsonPointer(final String logstashAttributeValue) {
        Matcher nestedSyntaxMatcher = NESTED_SYNTAX_PATTERN.matcher(logstashAttributeValue);
        if (nestedSyntaxMatcher.matches()) {
            return logstashAttributeValue.replace("\\]\\[", "/").replace("[", "/").replace("]", "");
        }
        return logstashAttributeValue;
    }
}
