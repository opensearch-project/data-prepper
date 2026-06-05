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
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum RecordType {
    STRING("string"),
    EVENT("event");

    private static final Map<String, RecordType> NAMES_MAP = Stream.of(values())
            .collect(Collectors.toMap(RecordType::toString, v -> v));

    private final String name;

    RecordType(final String name) {
        this.name = name;
    }

    @JsonValue
    @Override
    public String toString() {
        return name;
    }

    @JsonCreator
    public static RecordType fromString(final String name) {
        if (name == null) {
            throw new IllegalArgumentException("Invalid record_type: null. Valid values are: " + NAMES_MAP.keySet());
        }
        final RecordType value = NAMES_MAP.get(name.toLowerCase());
        if (value == null) {
            throw new IllegalArgumentException("Invalid record_type: " + name + ". Valid values are: " + NAMES_MAP.keySet());
        }
        return value;
    }
}
