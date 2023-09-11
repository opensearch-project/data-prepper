/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.compression;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public enum CompressionOption {
    NONE("none", null, NoneCompressionEngine::new),
    GZIP("gzip", "gz", GZipCompressionEngine::new),
    SNAPPY("snappy", "snappy", SnappyCompressionEngine::new);

    private static final Map<String, CompressionOption> OPTIONS_MAP = Arrays.stream(CompressionOption.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    private final String option;

    private final String extension;
    private final Supplier<CompressionEngine> compressionEngineSupplier;
    CompressionOption(final String option, String extension, final Supplier<CompressionEngine> compressionEngineSupplier) {
        this.option = option.toLowerCase();
        this.extension = extension;
        this.compressionEngineSupplier = compressionEngineSupplier;
    }

    public CompressionEngine getCompressionEngine() {
        return compressionEngineSupplier.get();
    }

    public String getOption() {
        return option;
    }

    public Optional<String> getExtension() {
        return Optional.ofNullable(extension);
    }

    @JsonCreator
    public static CompressionOption fromOptionValue(final String option) {
        return OPTIONS_MAP.get(option);
    }
}
