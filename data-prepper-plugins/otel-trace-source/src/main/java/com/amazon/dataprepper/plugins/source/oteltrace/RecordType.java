/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.oteltrace;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * An enum to represent the record data types supported in {@link OTelTraceSource}.
 * @since 1.4
 * TODO: remove in 2.0
 */
enum RecordType {
    otlp,
    event;

    private static final Map<String, RecordType> NAMES_MAP = new HashMap<>();

    static {
        Arrays.stream(RecordType.values()).forEach(recordType -> NAMES_MAP.put(recordType.name(), recordType));
    }

    public static boolean contains(final String name) {
        return NAMES_MAP.containsKey(name);
    }
}
