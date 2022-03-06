/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.opensearch.dataprepper.logstash.mapping.GrokNamedCapturesUtil.GrokNamedCapturesPair;

public class GrokNamedCapturesUtilTest {
    private final String randomPrefix = UUID.randomUUID().toString();
    private final String randomSuffix = UUID.randomUUID().toString();
    private final String randomMiddle = UUID.randomUUID().toString();

    private final List<String> namedCapturesPatterns = new ArrayList<>();
    private final String firstNamedCapturesPattern =  UUID.randomUUID().toString();
    private final String secondNamedCapturesPattern =  UUID.randomUUID().toString();

    private final List<String> namedCapturesNames = new ArrayList<>();
    private final String firstNamedCapturesName = UUID.randomUUID().toString();
    private final String secondNamedCapturesName = UUID.randomUUID().toString();


    @BeforeEach
    public void setup() {
        namedCapturesNames.add(firstNamedCapturesName);
        namedCapturesNames.add(secondNamedCapturesName);

        namedCapturesPatterns.add(firstNamedCapturesPattern);
        namedCapturesPatterns.add(secondNamedCapturesPattern);
    }

    @Test
    public void testSingleNamedCaptures() {
        final String namedCapturesPattern =  UUID.randomUUID().toString();
        final String namedCapturesName = UUID.randomUUID().toString();
        final String regex = String.format("(?<%s>%s)", namedCapturesName, namedCapturesPattern);
        final GrokNamedCapturesPair result = GrokNamedCapturesUtil.convertRegexNamedCapturesToGrokPatternDefinitions(regex);


        assertThat(result.getMappedPatternDefinitions().size(), equalTo(1));

        for (final Map.Entry<String, String> patternDefinition : result.getMappedPatternDefinitions().entrySet()) {
           assertThat(patternDefinition.getValue().equals(namedCapturesPattern), equalTo(true));
           final String expectedResult = String.format("%%{%s:%s}", patternDefinition.getKey(), namedCapturesName);
           assertThat(result.getMappedRegex().equals(expectedResult), equalTo(true));
        }
    }

    @Test
    public void testConnectedNamedCaptures() {
        final String regex = String.format("%s(?<%s>%s)(?<%s>%s)",
                randomPrefix, firstNamedCapturesName, firstNamedCapturesPattern,
                secondNamedCapturesName, secondNamedCapturesPattern);

        final GrokNamedCapturesPair result = GrokNamedCapturesUtil.convertRegexNamedCapturesToGrokPatternDefinitions(regex);
        assertThat(result.getMappedPatternDefinitions().size(), equalTo(2));

        int index = 0;
        final List<String> patternDefinitionNames = new ArrayList<>();
        for (final Map.Entry<String, String> patternDefinition : result.getMappedPatternDefinitions().entrySet()) {
            assertThat(patternDefinition.getValue().equals(namedCapturesPatterns.get(index)), equalTo(true));
            patternDefinitionNames.add(patternDefinition.getKey());
            index++;
        }
        final String expectedResult = String.format("%s%%{%s:%s}%%{%s:%s}", randomPrefix, patternDefinitionNames.get(0), namedCapturesNames.get(0), patternDefinitionNames.get(1), namedCapturesNames.get(1));
        assertThat(result.getMappedRegex().equals(expectedResult), equalTo(true));
    }

    @Test
    public void testSeparatedNamedCaptures() {
        final String regex = String.format("%s(?<%s>%s) %s (?<%s>%s)%s",
                randomPrefix, firstNamedCapturesName, firstNamedCapturesPattern, randomMiddle,
                secondNamedCapturesName, secondNamedCapturesPattern, randomSuffix);

        final GrokNamedCapturesPair result = GrokNamedCapturesUtil.convertRegexNamedCapturesToGrokPatternDefinitions(regex);
        assertThat(result.getMappedPatternDefinitions().size(), equalTo(2));

        int index = 0;
        final List<String> patternDefinitionNames = new ArrayList<>();
        for (final Map.Entry<String, String> patternDefinition : result.getMappedPatternDefinitions().entrySet()) {
            assertThat(patternDefinition.getValue().equals(namedCapturesPatterns.get(index)), equalTo(true));
            patternDefinitionNames.add(patternDefinition.getKey());
            index++;
        }
        final String expectedResult = String.format("%s%%{%s:%s} %s %%{%s:%s}%s", randomPrefix, patternDefinitionNames.get(0), namedCapturesNames.get(0), randomMiddle,
                patternDefinitionNames.get(1), namedCapturesNames.get(1), randomSuffix);
        assertThat(result.getMappedRegex().equals(expectedResult), equalTo(true));
    }

    @Test
    public void testNoNamedCapturesKeepsSameRegex() {
        final String regex = String.format("%s %s", UUID.randomUUID().toString(), UUID.randomUUID().toString());
        final GrokNamedCapturesPair result = GrokNamedCapturesUtil.convertRegexNamedCapturesToGrokPatternDefinitions(regex);
        assertThat(result.getMappedPatternDefinitions().size(), equalTo(0));
        assertThat(result.getMappedRegex().equals(regex), equalTo(true));
    }

    @Test
    public void testDuplicateNamedCaptures() {
        final String namedCapturesName = UUID.randomUUID().toString();
        final String namedCapturesPattern =  UUID.randomUUID().toString();

        final String regex = String.format("%s(?<%s>%s) %s (?<%s>%s)%s",
                randomPrefix, namedCapturesName, namedCapturesPattern, randomMiddle,
                namedCapturesName, namedCapturesPattern, randomSuffix);

        final GrokNamedCapturesPair result = GrokNamedCapturesUtil.convertRegexNamedCapturesToGrokPatternDefinitions(regex);
        assertThat(result.getMappedPatternDefinitions().size(), equalTo(2));

        final List<String> patternDefinitionNames = new ArrayList<>();
        for (final Map.Entry<String, String> patternDefinition : result.getMappedPatternDefinitions().entrySet()) {
            assertThat(patternDefinition.getValue().equals(namedCapturesPattern), equalTo(true));
            patternDefinitionNames.add(patternDefinition.getKey());
        }
        final String expectedResult = String.format("%s%%{%s:%s} %s %%{%s:%s}%s", randomPrefix, patternDefinitionNames.get(0), namedCapturesName, randomMiddle,
                patternDefinitionNames.get(1), namedCapturesName, randomSuffix);
        assertThat(result.getMappedRegex().equals(expectedResult), equalTo(true));
    }

    @Test
    public void testNamedCapturesWithNestedSyntax() {
        final String namedCapturesPattern =  UUID.randomUUID().toString();
        final String namedCapturesName = "[foo][bar]";
        final String regex = String.format("(?<%s>%s)", namedCapturesName, namedCapturesPattern);
        final GrokNamedCapturesPair result = GrokNamedCapturesUtil.convertRegexNamedCapturesToGrokPatternDefinitions(regex);


        assertThat(result.getMappedPatternDefinitions().size(), equalTo(1));

        for (final Map.Entry<String, String> patternDefinition : result.getMappedPatternDefinitions().entrySet()) {
            assertThat(patternDefinition.getValue().equals(namedCapturesPattern), equalTo(true));
            final String expectedResult = String.format("%%{%s:%s}", patternDefinition.getKey(), "/foo/bar");
            assertThat(result.getMappedRegex().equals(expectedResult), equalTo(true));
        }
    }
}
