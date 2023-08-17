/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.compression;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public enum CompressionOption {
    NONE("none", NoneCompressionEngine::new),
    GZIP("gzip", GZipCompressionEngine::new),
    SNAPPY("snappy", SnappyCompressionEngine::new);

    private static final Map<String, CompressionOption> OPTIONS_MAP = Arrays.stream(CompressionOption.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;

    private final Supplier<CompressionEngine> compressionEngineSupplier;
    CompressionOption(final String option, final Supplier<CompressionEngine> compressionEngineSupplier) {
        this.option = option.toLowerCase();
        this.compressionEngineSupplier = compressionEngineSupplier;
    }

    public CompressionEngine getCompressionEngine() {
        return compressionEngineSupplier.get();
    }

    String getOption() {
        return option;
    }

    @JsonCreator
    public static CompressionOption fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }
}
