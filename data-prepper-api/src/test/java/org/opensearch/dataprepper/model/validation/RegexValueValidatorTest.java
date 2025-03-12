/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.validation;

import jakarta.validation.ConstraintValidatorContext;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith(MockitoExtension.class)
class RegexValueValidatorTest {
    @Mock
    private ConstraintValidatorContext constraintValidatorContext;

    @ParameterizedTest
    @MethodSource("provideRegexAndExpectedResult")
    void testValidateRegex(final String delimiterRegex, final boolean isValid) {
        final RegexValueValidator objectUnderTest = new RegexValueValidator();
        assertThat(objectUnderTest.isValid(delimiterRegex, constraintValidatorContext), is(isValid));
    }

    private static Stream<Arguments> provideRegexAndExpectedResult() {
        return Stream.of(
                arguments(null, true),
                arguments("", true),
                arguments("abc", true),
                arguments("(abc", false)
        );
    }
}