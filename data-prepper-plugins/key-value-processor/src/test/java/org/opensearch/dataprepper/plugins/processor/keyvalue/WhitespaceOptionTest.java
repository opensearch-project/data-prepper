/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.keyvalue;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class WhitespaceOptionTest {
    @ParameterizedTest
    @EnumSource(WhitespaceOption.class)
    void fromWhitespaceName_returns_expected_value(final WhitespaceOption whitespaceOption) {
        assertThat(WhitespaceOption.fromWhitespaceName(whitespaceOption.getWhitespaceName()), equalTo(whitespaceOption));
    }

    @ParameterizedTest
    @EnumSource(WhitespaceOption.class)
    void getWhitespaceName_returns_non_empty_string_for_all_types(final WhitespaceOption whitespaceOption) {
        assertThat(whitespaceOption.getWhitespaceName(), notNullValue());
        assertThat(whitespaceOption.getWhitespaceName(), not(emptyString()));
    }

    @ParameterizedTest
    @ArgumentsSource(WhitespaceOptionToKnownName.class)
    void getWhitespaceName_returns_expected_name(final WhitespaceOption whitespaceOption, final String expectedString) {
        assertThat(whitespaceOption.getWhitespaceName(), equalTo(expectedString));
    }

    static class WhitespaceOptionToKnownName implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(WhitespaceOption.LENIENT, "lenient"),
                    arguments(WhitespaceOption.STRICT, "strict")
            );
        }
    }
}