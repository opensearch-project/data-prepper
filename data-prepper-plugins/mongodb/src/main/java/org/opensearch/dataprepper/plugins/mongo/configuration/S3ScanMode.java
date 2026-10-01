/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum S3ScanMode {
    IDLE_OPTIMIZED("idle_optimized"),
    CONTINUOUS("continuous");

    private static final Map<String, S3ScanMode> NAMES_MAP = Arrays.stream(S3ScanMode.values())
            .collect(Collectors.toMap(S3ScanMode::toString, v -> v));

    private final String name;

    S3ScanMode(final String name) {
        this.name = name;
    }

    @JsonValue
    public String toString() {
        return this.name;
    }

    @JsonCreator
    public static S3ScanMode fromString(final String name) {
        if (name == null) {
            throw new IllegalArgumentException("s3_scan_mode must not be null. Valid values are: " + NAMES_MAP.keySet());
        }
        final S3ScanMode mode = NAMES_MAP.get(name.toLowerCase());
        if (mode == null) {
            throw new IllegalArgumentException("Invalid s3_scan_mode: " + name + ". Valid values are: " + NAMES_MAP.keySet());
        }
        return mode;
    }
}
