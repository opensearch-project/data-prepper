/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.codec.multiline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Defines whether unmatched lines should be grouped with the previous or next matching line.
 */
public enum MultilineWhat {

    /**
     * Unmatched lines are appended to the previous matching line's event.
     */
    PREVIOUS("previous"),

    /**
     * Unmatched lines are prepended to the next matching line's event.
     */
    NEXT("next");

    private static final Map<String, MultilineWhat> OPTIONS_MAP = Arrays.stream(MultilineWhat.values())
            .collect(Collectors.toMap(MultilineWhat::toString, value -> value));

    private final String name;

    MultilineWhat(final String name) {
        this.name = name;
    }

    @JsonCreator
    public static MultilineWhat fromString(final String value) {
        final MultilineWhat result = OPTIONS_MAP.get(value.toLowerCase());
        if (result == null) {
            throw new IllegalArgumentException("Invalid value for 'what': " + value + ". Valid values are: " + OPTIONS_MAP.keySet());
        }
        return result;
    }

    @JsonValue
    @Override
    public String toString() {
        return name;
    }
}
