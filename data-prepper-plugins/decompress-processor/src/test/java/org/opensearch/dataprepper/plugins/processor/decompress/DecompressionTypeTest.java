/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.opensearch.dataprepper.model.codec.DecompressionEngine;
import org.opensearch.dataprepper.plugins.codec.GZipDecompressionEngine;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class DecompressionTypeTest {

    @ParameterizedTest
    @ArgumentsSource(EnumToStringNameArgumentsProvider.class)
    void fromOptionValue_returns_expected_DecompressionType(final DecompressionType expectedEnumValue, final String enumName) {
        assertThat(DecompressionType.fromOptionValue(enumName), equalTo(expectedEnumValue));
    }

    @ParameterizedTest
    @ArgumentsSource(EnumToDecompressionEngineClassArgumentsProvider.class)
    void getDecompressionEngine_returns_expected_DecompressionEngine(final DecompressionType enumValue, final Class<DecompressionEngine> decompressionEngineClass) {
        assertThat(enumValue.getDecompressionEngine(), instanceOf(decompressionEngineClass));
    }

    private static class EnumToStringNameArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(DecompressionType.GZIP, "gzip")
            );
        }
    }

    private static class EnumToDecompressionEngineClassArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(DecompressionType.GZIP, GZipDecompressionEngine.class)
            );
        }
    }
}
