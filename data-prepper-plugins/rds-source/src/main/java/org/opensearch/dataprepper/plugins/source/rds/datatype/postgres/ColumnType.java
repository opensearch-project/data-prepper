/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres;

import java.util.HashMap;
import java.util.Map;

public enum ColumnType {
    BOOLEAN(16, "boolean"),
    SMALLINT(21, "smallint"),
    INTEGER(23, "integer"),
    BIGINT(20, "bigint"),
    REAL(700, "real"),
    DOUBLE_PRECISION(701, "double precision"),
    NUMERIC(1700, "numeric"),
    TEXT(25, "text"),
    VARCHAR(1043, "varchar"),
    DATE(1082, "date"),
    TIME(1083, "time"),
    TIMESTAMP(1114, "timestamp"),
    TIMESTAMPTZ(1184, "timestamptz"),
    UUID(2950, "uuid"),
    JSON(114, "json"),
    JSONB(3802, "jsonb");

    private final int typeId;
    private final String typeName;

    private static final Map<Integer, ColumnType> TYPE_ID_MAP = new HashMap<>();

    static {
        for (ColumnType type : values()) {
            TYPE_ID_MAP.put(type.typeId, type);
        }
    }

    ColumnType(int typeId, String typeName) {
        this.typeId = typeId;
        this.typeName = typeName;
    }

    public int getTypeId() {
        return typeId;
    }

    public String getTypeName() {
        return typeName;
    }

    public static ColumnType getByTypeId(int typeId) {
        if (!TYPE_ID_MAP.containsKey(typeId)) {
            throw new IllegalArgumentException("Unsupported column type id: " + typeId);
        }
        return TYPE_ID_MAP.get(typeId);
    }

    public static String getTypeNameByEnum(ColumnType columnType) {
        return columnType.getTypeName();
    }
}
