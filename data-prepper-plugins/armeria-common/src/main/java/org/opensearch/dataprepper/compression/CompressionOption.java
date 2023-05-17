package org.opensearch.dataprepper.compression;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public enum CompressionOption {
    NONE("none"),
    GZIP("gzip");

    private final String option;

    private static final Map<String, CompressionOption> OPTIONS_MAP = Arrays.stream(CompressionOption.values())
            .collect(Collectors.toMap(
                    value -> value.option,
                    value -> value
            ));

    CompressionOption(final String option) {
        this.option = option.toLowerCase();
    }

    @JsonCreator
    static CompressionOption fromOptionValue(final String option) {
        return Optional.ofNullable(OPTIONS_MAP.get(option.toLowerCase())).orElseThrow(
                () -> new IllegalArgumentException("Unrecognized compression: " + option));
    }
}
