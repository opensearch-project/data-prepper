/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class HandleFailedEventsOptionTest {
    @ParameterizedTest
    @ArgumentsSource(EnumToShouldLogArgumentsProvider.class)
    void shouldLog_returns_expected_value(final HandleFailedEventsOption option, final boolean shouldLog) {
        assertThat(option.shouldLog(), equalTo(shouldLog));
    }

    @ParameterizedTest
    @ArgumentsSource(EnumToShouldShouldDropArgumentsProvider.class)
    void shouldDropEvent_returns_expected_value(final HandleFailedEventsOption option, final boolean shouldDrop) {
        assertThat(option.shouldDropEvent(), equalTo(shouldDrop));
    }

    @ParameterizedTest
    @ArgumentsSource(EnumToOptionValueArgumentsProvider.class)
    void toOptionValue_returns_expected_value(final HandleFailedEventsOption option, final String optionValue) {
        assertThat(option.toOptionValue(), equalTo(optionValue));
    }

    @ParameterizedTest
    @ArgumentsSource(EnumToOptionValueArgumentsProvider.class)
    void fromOptionValue_returns_expected_option(final HandleFailedEventsOption option, final String optionValue) {
        assertThat(HandleFailedEventsOption.fromOptionValue(optionValue), equalTo(option));
    }

    @ParameterizedTest
    @EnumSource(HandleFailedEventsOption.class)
    void toOptionValue_returns_non_null_for_all(final HandleFailedEventsOption option) {
        assertThat(option.toOptionValue(), notNullValue());
    }

    private static class EnumToOptionValueArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(HandleFailedEventsOption.SKIP, "skip"),
                    arguments(HandleFailedEventsOption.SKIP_SILENTLY, "skip_silently"),
                    arguments(HandleFailedEventsOption.DROP, "drop"),
                    arguments(HandleFailedEventsOption.DROP_SILENTLY, "drop_silently")
            );
        }
    }

    private static class EnumToShouldShouldDropArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(HandleFailedEventsOption.SKIP, false),
                    arguments(HandleFailedEventsOption.SKIP_SILENTLY, false),
                    arguments(HandleFailedEventsOption.DROP, true),
                    arguments(HandleFailedEventsOption.DROP_SILENTLY, true)
            );
        }
    }

    private static class EnumToShouldLogArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(HandleFailedEventsOption.SKIP, true),
                    arguments(HandleFailedEventsOption.DROP, true),
                    arguments(HandleFailedEventsOption.SKIP_SILENTLY, false),
                    arguments(HandleFailedEventsOption.DROP_SILENTLY, false)
            );
        }
    }
}
