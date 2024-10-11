/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

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

class OTelLogsFormatOptionTest {
    @ParameterizedTest
    @EnumSource(OTelLogsFormatOption.class)
    void fromFormatName_returns_expected_value(final OTelLogsFormatOption formatOption) {
        assertThat(OTelLogsFormatOption.fromFormatName(formatOption.getFormatName()), equalTo(formatOption));
    }

    @ParameterizedTest
    @EnumSource(OTelLogsFormatOption.class)
    void getFormatName_returns_non_empty_string_for_all_types(final OTelLogsFormatOption formatOption) {
        assertThat(formatOption.getFormatName(), notNullValue());
        assertThat(formatOption.getFormatName(), not(emptyString()));
    }

    @ParameterizedTest
    @ArgumentsSource(OTelLogsFormatOptionToKnownName.class)
    void getFormatName_returns_expected_name(final OTelLogsFormatOption formatOption, final String expectedString) {
        assertThat(formatOption.getFormatName(), equalTo(expectedString));
    }

    static class OTelLogsFormatOptionToKnownName implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(OTelLogsFormatOption.JSON, "json")
            );
        }
    }
}
