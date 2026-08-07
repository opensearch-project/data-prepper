/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.event;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An enum to represent the event types supported in Data Prepper.
 * @since 1.2
 */
public enum EventType {

    LOG("LOG"),
    TRACE("TRACE"),
    METRIC("METRIC"),

    DOCUMENT("DOCUMENT");

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
