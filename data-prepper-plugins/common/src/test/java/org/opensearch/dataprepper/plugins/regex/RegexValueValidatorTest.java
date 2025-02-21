/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.regex;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class RegexValueValidatorTest {
    @ParameterizedTest
    @MethodSource("provideRegexAndExpectedResult")
    void testValidateRegex(final String delimiterRegex, final boolean isValid) {
        assertThat(RegexValueValidator.validateRegex(delimiterRegex), is(isValid));
    }

    private static Stream<Arguments> provideRegexAndExpectedResult() {
        return Stream.of(
                Arguments.of(null, true),
                Arguments.of("", true),
                Arguments.of("abc", true),
                Arguments.of("(abc", false)
        );
    }
}