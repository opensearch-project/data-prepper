/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public enum EngineType {

    MYSQL("mysql"),
    AURORA_MYSQL("aurora-mysql");

    private static final Map<String, EngineType> ENGINE_TYPE_MAP = Arrays.stream(EngineType.values())
            .collect(Collectors.toMap(
                    value -> value.engine,
                    value -> value
            ));
    private final String engine;

    EngineType(String engine) {
        this.engine = engine;
    }

    @Override
    public String toString() {
        return engine;
    }

    @JsonCreator
    public static EngineType fromOptionValue(final String option) {
        return ENGINE_TYPE_MAP.get(option);
    }
}
