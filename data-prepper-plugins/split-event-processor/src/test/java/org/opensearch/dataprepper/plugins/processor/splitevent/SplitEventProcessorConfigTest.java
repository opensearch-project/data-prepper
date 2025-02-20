/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.splitevent;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class SplitEventProcessorConfigTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private SplitEventProcessorConfig createObjectUnderTest(final String field, final String delimiterRegex) {
        final Map<String, Object> map = Map.of(
                "field", field,
                "delimiter_regex", delimiterRegex
        );
        return OBJECT_MAPPER.convertValue(map, SplitEventProcessorConfig.class);
    }

    @ParameterizedTest
    @MethodSource("provideDelimiterRegexAndIsValid")
    void testIsDelimiterRegexValid(final String delimiterRegex, final boolean isValid) {
        final SplitEventProcessorConfig objectUnderTest = createObjectUnderTest("test_field", delimiterRegex);
        assertThat(objectUnderTest.isDelimiterRegexValid(), is(isValid));
    }

    private static Stream<Arguments> provideDelimiterRegexAndIsValid() {
        return Stream.of(
                Arguments.of(null, true),
                Arguments.of("", true),
                Arguments.of("abc", true),
                Arguments.of("(abc", false)
        );
    }
}