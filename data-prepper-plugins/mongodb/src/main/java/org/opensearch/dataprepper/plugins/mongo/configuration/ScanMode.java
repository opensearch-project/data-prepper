/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.mongo.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum ScanMode {
    IDLE_OPTIMIZED("idle_optimized"),
    CONTINUOUS("continuous");

    private static final Map<String, ScanMode> NAMES_MAP = Arrays.stream(ScanMode.values())
            .collect(Collectors.toMap(ScanMode::toString, v -> v));

    private final String name;

    ScanMode(final String name) {
        this.name = name;
    }

    @JsonValue
    public String toString() {
        return this.name;
    }

    @JsonCreator
    public static ScanMode fromString(final String name) {
        if (name == null) {
            throw new IllegalArgumentException("scan_mode must not be null. Valid values are: " + NAMES_MAP.keySet());
        }
        final ScanMode mode = NAMES_MAP.get(name.toLowerCase());
        if (mode == null) {
            throw new IllegalArgumentException("Invalid scan_mode: " + name + ". Valid values are: " + NAMES_MAP.keySet());
        }
        return mode;
    }
}
