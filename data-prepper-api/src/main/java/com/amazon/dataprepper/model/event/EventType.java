package com.amazon.dataprepper.model.event;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An enumm to represent the event types supported in Data Prepper.
 * @since 1.2
 */
public enum EventType {

    LOG("LOG"),
    TRACE("TRACE");

    private static final Map<String, EventType> NAMES_MAP = Arrays.stream(EventType.values())
            .collect(Collectors.toMap(EventType::toString, Function.identity()));

    private final String name;

    EventType(final String name) {
        this.name = name;
    }

    public String toString() {
        return this.name;
    }

    public static EventType getByName(final String name) {
        return NAMES_MAP.get(name.toUpperCase());
    }
}
