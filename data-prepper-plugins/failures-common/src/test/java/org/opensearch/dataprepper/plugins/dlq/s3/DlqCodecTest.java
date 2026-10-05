/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.dlq.s3;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class DlqCodecTest {
    @ParameterizedTest
    @EnumSource(DlqCodec.class)
    void fromOptionValue_returns_expected_value(final DlqCodec dlqCodec) {
        assertThat(DlqCodec.fromOptionValue(dlqCodec.getExtension()), equalTo(dlqCodec));
    }

    @ParameterizedTest
    @EnumSource(DlqCodec.class)
    void getExtension_returns_non_empty_string_for_all_types(final DlqCodec dlqCodec) {
        assertThat(dlqCodec.getExtension(), notNullValue());
        assertThat(dlqCodec.getExtension(), not(emptyString()));
    }

    @ParameterizedTest
    @ArgumentsSource(DlqCodecToKnownName.class)
    void getExtension_returns_expected_name(final DlqCodec dlqCodec, final String expectedString) {
        assertThat(dlqCodec.getExtension(), equalTo(expectedString));
    }

    @ParameterizedTest
    @ValueSource(strings = {"JSON", "Json", "NDJSON", "Ndjson", "yaml", "csv", ""})
    void fromOptionValue_throws_for_unknown_or_mismatched_case(final String invalidOption) {
        assertThrows(IllegalArgumentException.class, () -> DlqCodec.fromOptionValue(invalidOption));
    }

    static class DlqCodecToKnownName implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) {
            return Stream.of(
                    arguments(DlqCodec.JSON, "json"),
                    arguments(DlqCodec.NDJSON, "ndjson")
            );
        }
    }
}
