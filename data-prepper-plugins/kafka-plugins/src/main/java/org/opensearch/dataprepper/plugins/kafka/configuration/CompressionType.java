package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum CompressionType {
    NONE("none"),
    ZSTD("zstd");

    private static final Map<String, CompressionType> OPTIONS_MAP = Arrays.stream(CompressionType.values())
            .collect(Collectors.toMap(
                    value -> value.type,
                    value -> value
            ));

    private final String type;

    CompressionType(final String type) {
        this.type = type;
    }

    @JsonCreator
    static CompressionType fromTypeValue(final String type) {
        return OPTIONS_MAP.get(type.toLowerCase());
    }
}
