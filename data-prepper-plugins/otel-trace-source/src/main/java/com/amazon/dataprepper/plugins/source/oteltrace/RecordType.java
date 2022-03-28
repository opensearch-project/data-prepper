/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.oteltrace;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * An enum to represent the record data types supported in {@link OTelTraceSource}.
 * @since 1.4
 * TODO: remove in 2.0
 */
enum RecordType {
    @JsonProperty("otlp")
    OTLP("otlp"),
    @JsonProperty("event")
    EVENT("event");

    private static final Map<String, RecordType> NAMES_MAP = new HashMap<>();

    static {
        Arrays.stream(RecordType.values()).forEach(recordType -> NAMES_MAP.put(recordType.name(), recordType));
    }

    private final String name;

    RecordType(final String name) {
        this.name = name;
    }

    public String toString() {
        return this.name;
    }
}
