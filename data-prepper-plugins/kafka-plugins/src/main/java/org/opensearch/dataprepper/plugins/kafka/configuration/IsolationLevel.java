package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;

public enum IsolationLevel {
    READ_UNCOMMITTED("read_uncommitted"),
    READ_COMMITTED("read_committed");

    private static final Map<String, IsolationLevel> OPTIONS_MAP = Arrays.stream(IsolationLevel.values())
            .collect(Collectors.toMap(
                    value -> value.type,
                    value -> value
            ));

    private final String type;

    IsolationLevel(final String type) {
        this.type = type;
    }

    @JsonCreator
    public static IsolationLevel fromTypeValue(final String type) {
        return OPTIONS_MAP.get(type.toLowerCase());
    }

    public String getType() {
        return type;
    }
}
