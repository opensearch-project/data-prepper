/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GrokMatchUtil {
    private static final String GROK_MATCH_PATTERN_REGEX = "(?<openingBrackets>%\\{)(?<grokSyntax>[^{}:]+)(?<separator1>:)?" +
            "(?<grokSemantic>[^{}:]+)?(?<separator2>:)?(?<dataType>[a-zA-Z]+)?(?<closingBrackets>}(\\s+)?)";
    private static final Pattern GROK_MATCH_PATTERN = Pattern.compile(GROK_MATCH_PATTERN_REGEX);

    public static String convertGrokMatchPattern(final String matchPattern) {
        Matcher grokMatchPatternMatcher = GROK_MATCH_PATTERN.matcher(matchPattern);
        StringBuilder convertedGrokMatchPattern = new StringBuilder();
        while(grokMatchPatternMatcher.find()) {
            convertedGrokMatchPattern.append(grokMatchPatternMatcher.group("openingBrackets"));
            convertedGrokMatchPattern.append(grokMatchPatternMatcher.group("grokSyntax"));
            if (grokMatchPatternMatcher.group("separator1") != null)
                convertedGrokMatchPattern.append(grokMatchPatternMatcher.group("separator1"));
            if (grokMatchPatternMatcher.group("grokSemantic") != null)
                convertedGrokMatchPattern.append(NestedSyntaxConverter.convertNestedSyntaxToJsonPath(grokMatchPatternMatcher.group("grokSemantic")));
            if (grokMatchPatternMatcher.group("separator2") != null)
                convertedGrokMatchPattern.append(grokMatchPatternMatcher.group("separator2"));
            if (grokMatchPatternMatcher.group("dataType") != null)
                convertedGrokMatchPattern.append(grokMatchPatternMatcher.group("dataType"));
            convertedGrokMatchPattern.append(grokMatchPatternMatcher.group("closingBrackets"));
        }
        if (convertedGrokMatchPattern.toString().isEmpty())
            return matchPattern;

        return convertedGrokMatchPattern.toString();
    }
}
