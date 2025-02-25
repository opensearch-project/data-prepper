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

    MYSQL("mysql", EngineCategory.MYSQL, EnginePlatform.RDS),
    POSTGRES("postgresql", EngineCategory.POSTGRES, EnginePlatform.RDS),
    AURORA_MYSQL("aurora-mysql", EngineCategory.MYSQL, EnginePlatform.AURORA),
    AURORA_POSTGRES("aurora-postgresql", EngineCategory.POSTGRES, EnginePlatform.AURORA);

    private static final Map<String, EngineType> ENGINE_TYPE_MAP = Arrays.stream(EngineType.values())
            .collect(Collectors.toMap(
                    value -> value.engine,
                    value -> value
            ));
    private final String engine;
    private final EngineCategory category;
    private final EnginePlatform platform;

    EngineType(String engine, EngineCategory category, EnginePlatform platform) {
        this.engine = engine;
        this.category = category;
        this.platform = platform;
    }

    @Override
    public String toString() {
        return engine;
    }

    @JsonCreator
    public static EngineType fromString(final String option) {
        return ENGINE_TYPE_MAP.get(option);
    }

    public enum EngineCategory {
        MYSQL,
        POSTGRES
    }

    public enum EnginePlatform {
        RDS,
        AURORA
    }

    public boolean isAurora() {
        return platform == EnginePlatform.AURORA;
    }

    public boolean isRds() {
        return platform == EnginePlatform.RDS;
    }

    public boolean isMySql() {
        return category == EngineCategory.MYSQL;
    }

    public boolean isPostgres() {
        return category == EngineCategory.POSTGRES;
    }
}
