/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionOption;

import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class CompressionConverterTest {
    @ParameterizedTest
    @ArgumentsSource(OptionToParquetCodec.class)
    void convertCodec_with_known_codecs(final CompressionOption compressionOption, final CompressionCodecName expectedParquetCodec) {
        assertThat(CompressionConverter.convertCodec(compressionOption), equalTo(expectedParquetCodec));
    }

    static class OptionToParquetCodec implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(CompressionOption.NONE, CompressionCodecName.UNCOMPRESSED),
                    arguments(CompressionOption.GZIP, CompressionCodecName.GZIP),
                    arguments(CompressionOption.SNAPPY, CompressionCodecName.SNAPPY)
            );
        }
    }
}