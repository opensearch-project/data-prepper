/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GrokMatchUtil {
    private static final String GROK_MATCH_PATTERN_REGEX = "(%\\{[^{}:]+)(:)?([^{}]+)?(}(\\s+)?)";
    private static final Pattern GROK_MATCH_PATTERN = Pattern.compile(GROK_MATCH_PATTERN_REGEX);

    public static String convertGrokMatchPattern(final String matchPattern) {
        Matcher grokMatchPatternMatcher = GROK_MATCH_PATTERN.matcher(matchPattern);
        StringBuilder convertedGrokMatchPattern = new StringBuilder();
        while(grokMatchPatternMatcher.find()) {
            convertedGrokMatchPattern.append(grokMatchPatternMatcher.group(1));
            if (grokMatchPatternMatcher.group(2) != null)
                convertedGrokMatchPattern.append(grokMatchPatternMatcher.group(2));
            if (grokMatchPatternMatcher.group(3) != null)
                convertedGrokMatchPattern.append(NestedSyntaxConverter.convertNestedSyntaxToJsonPath(grokMatchPatternMatcher.group(3)));
            convertedGrokMatchPattern.append(grokMatchPatternMatcher.group(4));
        }
        if (convertedGrokMatchPattern.toString().isEmpty())
            return matchPattern;

        return convertedGrokMatchPattern.toString();
    }
}
