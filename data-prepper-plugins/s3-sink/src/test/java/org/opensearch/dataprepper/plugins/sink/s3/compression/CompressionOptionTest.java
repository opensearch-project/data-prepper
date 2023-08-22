/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.compression;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Optional;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class CompressionOptionTest {
    @ParameterizedTest
    @EnumSource(CompressionOption.class)
    void fromOptionValue_returns_expected_value(final CompressionOption option) {
        assertThat(CompressionOption.fromOptionValue(option.getOption()), equalTo(option));
    }

    @ParameterizedTest
    @EnumSource(CompressionOption.class)
    void getCompressionEngine_returns_a_CompressionEngine(final CompressionOption option) {
        assertThat(option.getCompressionEngine(), instanceOf(CompressionEngine.class));
    }

    @ParameterizedTest
    @ArgumentsSource(OptionToExpectedEngine.class)
    void getCompressionEngine_returns_expected_engine_type(final CompressionOption option, final Class<CompressionEngine> expectedEngineType) {
        assertThat(option.getCompressionEngine(), instanceOf(expectedEngineType));
    }

    @ParameterizedTest
    @EnumSource(CompressionOption.class)
    void getExtension_returns_non_null_Optional(final CompressionOption option) {
        assertThat(option.getExtension(), notNullValue());
    }

    @ParameterizedTest
    @ArgumentsSource(OptionToExpectedExtension.class)
    void getExtension_returns_expected_extension(final CompressionOption option, final String expectedExtension) {
        Optional<String> extension = option.getExtension();
        assertThat(extension, notNullValue());
        assertThat(extension.isEmpty(), equalTo(false));
        assertThat(extension.get(), equalTo(expectedExtension));
    }

    @ParameterizedTest
    @EnumSource(value = CompressionOption.class, names = {"NONE"})
    void getExtension_returns_empty_Optional_when_no_extension(final CompressionOption option) {
        Optional<String> extension = option.getExtension();
        assertThat(extension, notNullValue());
        assertThat(extension.isEmpty(), equalTo(true));
    }

    static class OptionToExpectedEngine implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(CompressionOption.NONE, NoneCompressionEngine.class),
                    arguments(CompressionOption.GZIP, GZipCompressionEngine.class),
                    arguments(CompressionOption.SNAPPY, SnappyCompressionEngine.class)
            );
        }
    }

    static class OptionToExpectedExtension implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(CompressionOption.GZIP, "gz"),
                    arguments(CompressionOption.SNAPPY, "snappy")
            );
        }
    }
}