/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

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

public class ExtensionOptionTest {
    @ParameterizedTest
    @EnumSource(ExtensionOption.class)
    void fromExtension_returns_expected_value(final ExtensionOption extensionOption) {
        assertThat(ExtensionOption.fromExtension(extensionOption.getExtension()), equalTo(extensionOption));
    }

    @ParameterizedTest
    @EnumSource(ExtensionOption.class)
    void getExtension_returns_non_empty_null_for_all_types(final ExtensionOption extensionOption) {
        assertThat(extensionOption.getExtension(), notNullValue());
    }

    @ParameterizedTest
    @EnumSource(value = ExtensionOption.class)
    void getExtension_returns_non_empty_string_for_all_types_except_none(final ExtensionOption extensionOption) {
        assertThat(extensionOption.getExtension(), notNullValue());
        assertThat(extensionOption.getExtension(), not(emptyString()));
    }

    @ParameterizedTest
    @ArgumentsSource(ExtensionOptionToKnownName.class)
    void getTransformName_returns_expected_name(final ExtensionOption extensionOption, final String expectedString) {
        assertThat(extensionOption.getExtension(), equalTo(expectedString));
    }

    static class ExtensionOptionToKnownName implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(ExtensionOption.NDJSON, "ndjson"),
                    arguments(ExtensionOption.JSONL, "jsonl")
            );
        }
    }
}
