/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class RateLimiterModeTest {

    @ParameterizedTest
    @EnumSource(RateLimiterMode.class)
    void fromOptionValue(final RateLimiterMode value) {
        assertThat(RateLimiterMode.fromOptionValue(value.name()), is(value));
        assertThat(value, instanceOf(RateLimiterMode.class));
    }

    @ParameterizedTest
    @ArgumentsSource(RateLimiterModeToKnownName.class)
    void fromOptionValue_returns_expected_value(final RateLimiterMode rateLimiterMode, final String knownString) {
        assertThat(RateLimiterMode.fromOptionValue(knownString), equalTo(rateLimiterMode));
    }

    @ParameterizedTest
    @EnumSource(RateLimiterMode.class)
    void getOptionValue_returns_non_empty_string_for_all_types(final RateLimiterMode rateLimiterMode) {
        assertThat(rateLimiterMode.getOptionValue(), notNullValue());
        assertThat(rateLimiterMode.getOptionValue(), not(emptyString()));
    }

    @ParameterizedTest
    @ArgumentsSource(RateLimiterModeToKnownName.class)
    void getOptionValue_returns_expected_name(final RateLimiterMode rateLimiterMode, final String expectedString) {
        assertThat(rateLimiterMode.getOptionValue(), equalTo(expectedString));
    }

    static class RateLimiterModeToKnownName implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(RateLimiterMode.DROP, "drop"),
                    arguments(RateLimiterMode.BLOCK, "block")
            );
        }
    }
}
