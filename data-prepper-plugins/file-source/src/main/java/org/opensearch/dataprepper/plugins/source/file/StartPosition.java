/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.file;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum StartPosition {
    BEGINNING("beginning"),
    END("end");

    private static final Map<String, StartPosition> NAMES_MAP = Stream.of(values())
            .collect(Collectors.toMap(StartPosition::toString, v -> v));

    private final String name;

    StartPosition(final String name) {
        this.name = name;
    }

    @JsonCreator
    public static StartPosition fromString(final String name) {
        if (name == null) {
            throw new IllegalArgumentException("Invalid start_position: null. Valid values are: " + NAMES_MAP.keySet());
        }
        final StartPosition value = NAMES_MAP.get(name.toLowerCase());
        if (value == null) {
            throw new IllegalArgumentException("Invalid start_position: " + name + ". Valid values are: " + NAMES_MAP.keySet());
        }
        return value;
    }

    @Override
    public String toString() {
        return name;
    }
}
