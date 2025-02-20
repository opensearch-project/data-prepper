/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.event.EventKey;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class SplitStringProcessorConfigTest {
    @Mock
    private EventKey eventKey;

    @ParameterizedTest
    @MethodSource("provideDelimiterRegexAndIsValid")
    void testIsDelimiterRegexValid(final String delimiterRegex, final boolean isValid) {
        final SplitStringProcessorConfig.Entry objectUnderTest = new SplitStringProcessorConfig.Entry(
                eventKey, delimiterRegex, null, null);
        assertThat(objectUnderTest.isDelimiterRegexValid(), is(isValid));
    }

    private static Stream<Arguments> provideDelimiterRegexAndIsValid() {
        return Stream.of(
                Arguments.of("", true),
                Arguments.of("abc", true),
                Arguments.of("(abc", false)
        );
    }
}