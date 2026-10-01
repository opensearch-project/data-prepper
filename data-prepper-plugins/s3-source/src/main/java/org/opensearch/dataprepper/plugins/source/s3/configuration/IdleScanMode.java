/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.s3.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum IdleScanMode {
    IDLE_OPTIMIZED("idle_optimized"),
    CONTINUOUS("continuous");

    private static final Map<String, IdleScanMode> NAMES_MAP = Arrays.stream(IdleScanMode.values())
            .collect(Collectors.toMap(IdleScanMode::toString, v -> v));

    private final String name;

    IdleScanMode(final String name) {
        this.name = name;
    }

    @JsonValue
    public String toString() {
        return this.name;
    }

    @JsonCreator
    public static IdleScanMode fromString(final String name) {
        final IdleScanMode mode = NAMES_MAP.get(name.toLowerCase());
        if (mode == null) {
            throw new IllegalArgumentException("Invalid idle_scan_mode: " + name + ". Valid values are: " + NAMES_MAP.keySet());
        }
        return mode;
    }
}
