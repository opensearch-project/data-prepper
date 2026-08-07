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

public enum CheckpointStatus {
    ACTIVE("ACTIVE"),
    COMPLETED("COMPLETED");

    private static final Map<String, CheckpointStatus> NAMES_MAP = Stream.of(values())
            .collect(Collectors.toMap(CheckpointStatus::getValue, v -> v));

    private final String value;

    CheckpointStatus(final String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static CheckpointStatus fromString(final String value) {
        if (value == null) {
            throw new IllegalArgumentException("Invalid checkpoint status: null. Valid values are: " + NAMES_MAP.keySet());
        }
        final CheckpointStatus status = NAMES_MAP.get(value.toUpperCase());
        if (status == null) {
            throw new IllegalArgumentException("Invalid checkpoint status: " + value + ". Valid values are: " + NAMES_MAP.keySet());
        }
        return status;
    }
}
