/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class NestedSyntaxConverter {
    private static final String NESTED_SYNTAX_REGEX = "(\\[([^\\]\\[]+)\\])+";
    private static final Pattern NESTED_SYNTAX_PATTERN = Pattern.compile(NESTED_SYNTAX_REGEX);
    private NestedSyntaxConverter() {}

    public static Object convertNestedSyntaxToJsonPath(final Object logstashAttributeValue) {
        Matcher nestedSyntaxMatcher = NESTED_SYNTAX_PATTERN.matcher(logstashAttributeValue.toString());
        if (nestedSyntaxMatcher.matches()) {
            return logstashAttributeValue.toString().replace("\\]\\[", "/").replace("[", "/").replace("]", "");
        }
        return logstashAttributeValue;
    }
}
