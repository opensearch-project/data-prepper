/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.decompress.encoding;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class EncodingTypeTest {

    @ParameterizedTest
    @ArgumentsSource(EnumToStringNameArgumentsProvider.class)
    void fromOptionValue_returns_expected_DecompressionType(final EncodingType expectedEnumValue, final String enumName) {
        assertThat(EncodingType.fromOptionValue(enumName), equalTo(expectedEnumValue));
    }

    @ParameterizedTest
    @ArgumentsSource(EnumToDecoderEngineClassArgumentsProvider.class)
    void getDecompressionEngine_returns_expected_DecompressionEngine(final EncodingType enumValue, final Class<DecoderEngine> decoderEngineClass) {
        assertThat(enumValue.getDecoderEngine(), instanceOf(decoderEngineClass));
    }

    private static class EnumToStringNameArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(EncodingType.BASE64, "base64")
            );
        }
    }

    private static class EnumToDecoderEngineClassArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(EncodingType.BASE64, Base64DecoderEngine.class)
            );
        }
    }
}
