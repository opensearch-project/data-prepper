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
    BOOLEAN(16, "bool"),
    SMALLINT(21, "int2"),
    INTEGER(23, "int4"),
    BIGINT(20, "int8"),
    REAL(700, "float4"),
    DOUBLE_PRECISION(701, "float8"),
    NUMERIC(1700, "numeric"),
    TEXT(25, "text"),
    BPCHAR(1042, "bpchar"),
    VARCHAR(1043, "varchar"),
    DATE(1082, "date"),
    TIME(1083, "time"),
    TIMETZ(1266, "timetz"),
    TIMESTAMP(1114, "timestamp"),
    TIMESTAMPTZ(1184, "timestamptz"),
    INTERVAL(1186, "interval"),
    JSON(114, "json"),
    JSONB(3802, "jsonb"),
    JSONPATH(4072, "jsonpath"),
    MONEY(790,"money"),
    BIT(1560, "bit"),
    VARBIT(1562, "varbit"),
    POINT(600,"point"),
    LINE(628,"line"),
    LSEG(601,"lseg"),
    BOX(603, "box"),
    PATH(602, "path"),
    POLYGON(604, "polygon"),
    CIRCLE(718, "circle"),
    CIDR(650, "cidr"),
    INET(869, "inet"),
    MACADDR(829, "macaddr"),
    MACADDR8(774, "macaddr8"),
    XML(142, "xml"),
    UUID(2950, "uuid"),
    PG_LSN(3220, "pg_lsn"),
    PG_SNAPSHOT(5038, "pg_snapshot"),
    TXID_SNAPSHOT(2970, "txid_snapshot"),
    TSVECTOR(3614, "tsvector"),
    TSQUERY(3615, "tsquery"),
    BYTEA(17, "bytea"),
    INT4RANGE(3904, "int4range"),
    INT8RANGE(3926, "int8range"),
    TSRANGE(3908, "tsrange"),
    TSTZRANGE(3910, "tstzrange"),
    DATERANGE(3912, "daterange"),
    ENUM(-1,"enum");

    public static final int ENUM_TYPE_ID = -1;
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
