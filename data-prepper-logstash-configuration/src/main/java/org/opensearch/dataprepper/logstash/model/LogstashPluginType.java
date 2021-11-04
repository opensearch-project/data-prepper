package org.opensearch.dataprepper.logstash.model;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Types of plugins in Logstash configuration
 *
 * @since 1.2
 */
public enum LogstashPluginType {
    INPUT("input"),
    FILTER("filter"),
    OUTPUT("output");

    private final String value;

    private static final Map<String, LogstashPluginType> VALUES_MAP = Arrays.stream(LogstashPluginType.values())
            .collect(Collectors.toMap(LogstashPluginType::toString, Function.identity()));

    LogstashPluginType(final String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public static LogstashPluginType getByValue(final String value) {
        return VALUES_MAP.get(value.toLowerCase());
    }
}
