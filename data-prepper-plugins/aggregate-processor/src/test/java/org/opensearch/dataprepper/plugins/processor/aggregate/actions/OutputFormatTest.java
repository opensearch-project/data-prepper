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

public class OutputFormatTest {
    @ParameterizedTest
    @EnumSource(OutputFormat.class)
    void fromOptionValue(final OutputFormat value) {
        assertThat(OutputFormat.fromOptionValue(value.name()), is(value));
        assertThat(value, instanceOf(OutputFormat.class));
    }

    @ParameterizedTest
    @ArgumentsSource(OutputFormatToKnownName.class)
    void fromOptionValue_returns_expected_value(final OutputFormat outputFormat, final String knownString) {
        assertThat(OutputFormat.fromOptionValue(knownString), equalTo(outputFormat));
    }

    @ParameterizedTest
    @EnumSource(OutputFormat.class)
    void getOptionValue_returns_non_empty_string_for_all_types(final OutputFormat outputFormat) {
        assertThat(outputFormat.getOptionValue(), notNullValue());
        assertThat(outputFormat.getOptionValue(), not(emptyString()));
    }

    @ParameterizedTest
    @ArgumentsSource(OutputFormatToKnownName.class)
    void getOptionValue_returns_expected_name(final OutputFormat outputFormat, final String expectedString) {
        assertThat(outputFormat.getOptionValue(), equalTo(expectedString));
    }

    static class OutputFormatToKnownName implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(OutputFormat.OTEL_METRICS, "otel_metrics"),
                    arguments(OutputFormat.RAW, "raw")
            );
        }
    }
}
