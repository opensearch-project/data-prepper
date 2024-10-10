/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum OutputFormat {
    OTEL_METRICS("otel_metrics"),
    RAW("raw");

    private static final Map<String, OutputFormat> ACTIONS_MAP = Arrays.stream(OutputFormat.values())
        .collect(Collectors.toMap(
                value -> value.name,
                value -> value
        ));

    private final String name;

    OutputFormat(String name) {
        this.name = name.toLowerCase();
    }

    @Override
    public String toString() {
        return name;
    }

    @JsonCreator
    static OutputFormat fromOptionValue(final String option) {
        return ACTIONS_MAP.get(option.toLowerCase());
    }

    @JsonValue
    public String getOptionValue() {
        return name;
    }
}
