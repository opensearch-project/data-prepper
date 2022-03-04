/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;


import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class GrokNamedCapturesUtil {

    private static final String namedCapturesRegex = "\\(\\?\\<(.+?)\\>(.+?)\\)";
    private static final Pattern namedCapturesPattern = Pattern.compile(namedCapturesRegex);
    private static final int PATTERN_NAME_LENGTH = 8;

    static GrokNamedCapturesPair convertRegexNamedCapturesToGrokPatternDefinitions(String regexPattern) {
        Objects.requireNonNull(regexPattern);
        final Matcher matcher = namedCapturesPattern.matcher(regexPattern);
        final Map<String, String> mappedPatternDefinitions = new LinkedHashMap<>();
        while (matcher.find()) {
            final String patternRegex = matcher.group(2);
            final String captureName = NestedSyntaxConverter.convertNestedSyntaxToJsonPointer(matcher.group(1));
            final String patternName = GrokNamedCapturesUtil.generateRandomPatternName();
            final String replacementPattern = String.format("%%{%s:%s}", patternName, captureName);
            regexPattern = StringUtils.replaceOnce(regexPattern, matcher.group(0), replacementPattern);
            mappedPatternDefinitions.put(patternName, patternRegex);
        }
        return new GrokNamedCapturesPair(regexPattern, mappedPatternDefinitions);
    }

    private static String generateRandomPatternName() {
        return RandomStringUtils.random(PATTERN_NAME_LENGTH, true, true);
    }

    static class GrokNamedCapturesPair {
        private final String mappedRegex;
        private final Map<String, String> mappedPatternDefinitions;

        public GrokNamedCapturesPair(final String mappedRegex, final Map<String, String> mappedPatternDefinitions) {
            this.mappedRegex = mappedRegex;
            this.mappedPatternDefinitions = mappedPatternDefinitions;
        }

        public String getMappedRegex() {
            return mappedRegex;
        }

        public Map<String, String> getMappedPatternDefinitions() {
            return Collections.unmodifiableMap(mappedPatternDefinitions);
        }
    }
}
