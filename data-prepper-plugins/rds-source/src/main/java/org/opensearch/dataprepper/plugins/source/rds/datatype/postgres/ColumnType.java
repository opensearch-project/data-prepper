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

import org.postgresql.core.Oid;
import java.util.HashMap;
import java.util.Map;

public enum ColumnType {
    BOOLEAN(Oid.BOOL, "bool"),
    SMALLINT(Oid.INT2, "int2"),
    INTEGER(Oid.INT4, "int4"),
    BIGINT(Oid.INT8, "int8"),
    REAL(Oid.FLOAT4, "float4"),
    DOUBLE_PRECISION(Oid.FLOAT8, "float8"),
    NUMERIC(Oid.NUMERIC, "numeric"),

    TEXT(Oid.TEXT, "text"),
    BPCHAR(Oid.BPCHAR, "bpchar"),
    VARCHAR(Oid.VARCHAR, "varchar"),

    DATE(Oid.DATE, "date"),
    TIME(Oid.TIME, "time"),
    TIMETZ(Oid.TIMETZ, "timetz"),
    TIMESTAMP(Oid.TIMESTAMP, "timestamp"),
    TIMESTAMPTZ(Oid.TIMESTAMPTZ, "timestamptz"),
    INTERVAL(Oid.INTERVAL, "interval"),

    JSON(Oid.JSON, "json"),
    JSONB(Oid.JSONB, "jsonb"),
    JSONPATH(4072, "jsonpath"),

    MONEY(Oid.MONEY,"money"),

    BIT(Oid.BIT, "bit"),
    VARBIT(Oid.VARBIT, "varbit"),

    POINT(Oid.POINT,"point"),
    LINE(Oid.LINE,"line"),
    LSEG(Oid.LSEG,"lseg"),
    BOX(Oid.BOX, "box"),
    PATH(Oid.PATH, "path"),
    POLYGON(Oid.POLYGON, "polygon"),
    CIRCLE(Oid.CIRCLE, "circle"),

    CIDR(Oid.CIDR, "cidr"),
    INET(Oid.INET, "inet"),
    MACADDR(Oid.MACADDR, "macaddr"),
    MACADDR8(Oid.MACADDR8, "macaddr8"),

    XML(Oid.XML, "xml"),
    UUID(Oid.UUID, "uuid"),
    PG_LSN(3220, "pg_lsn"),
    PG_SNAPSHOT(5038, "pg_snapshot"),
    TXID_SNAPSHOT(2970, "txid_snapshot"),
    TSVECTOR(Oid.TSVECTOR, "tsvector"),
    TSQUERY(Oid.TSQUERY, "tsquery"),

    BYTEA(Oid.BYTEA, "bytea"),

    INT4RANGE(3904, "int4range"),
    INT8RANGE(3926, "int8range"),
    TSRANGE(3908, "tsrange"),
    TSTZRANGE(3910, "tstzrange"),
    DATERANGE(3912, "daterange"),
    NUMRANGE(3906, "numrange"),
    INT4MULTIRANGE(4451, "int4multirange"),
    INT8MULTIRANGE(4536, "int8multirange"),
    NUMMULTIRANGE(4532, "nummultirange"),
    DATEMULTIRANGE(4535, "datemultirange"),
    TSMULTIRANGE(4533, "tsmultirange"),
    TSTZMULTIRANGE(4534, "tstzmultirange"),

    INT2ARRAY(Oid.INT2_ARRAY,"_int2"),
    INT4ARRAY(Oid.INT4_ARRAY, "_int4"),
    INT8ARRAY(Oid.INT8_ARRAY, "_int8"),
    NUMERICARRAY(Oid.NUMERIC_ARRAY, "_numeric"),
    FLOAT4ARRAY(Oid.FLOAT4_ARRAY, "_float4"),
    FLOAT8ARRAY(Oid.FLOAT8_ARRAY, "_float8"),
    MONEYARRAY(Oid.MONEY_ARRAY, "_money"),

    TEXTARRAY(Oid.TEXT_ARRAY, "_text"),
    BPCHARARRAY(Oid.BPCHAR_ARRAY, "_bpchar"),
    VARCHARARRAY(Oid.VARCHAR_ARRAY, "_varchar"),

    BITARRAY(Oid.BIT_ARRAY, "_bit"),
    VARBITARRAY(Oid.VARBIT_ARRAY, "_varbit"),

    BOOLARRAY(Oid.BOOL_ARRAY, "_bool"),

    BYTEAARRAY(Oid.BYTEA_ARRAY, "_bytea"),

    DATEARRAY(Oid.DATE_ARRAY, "_date"),
    TIMEARRAY(Oid.TIME_ARRAY, "_time"),
    TIMETZARRAY(Oid.TIMETZ_ARRAY, "_timetz"),
    TIMESTAMPARRAY(Oid.TIMESTAMP_ARRAY, "_timestamp"),
    TIMESTAMPTZARRAY(Oid.TIMESTAMPTZ_ARRAY, "_timestamptz"),
    INTERVALARRAY(Oid.INTERVAL_ARRAY, "_interval"),

    POINTARRAY(Oid.POINT_ARRAY, "_point"),
    LINEARRAY(629, "_line"),
    LSEGARRAY(1018, "_lseg"),
    BOXARRAY(Oid.BOX_ARRAY, "_box"),
    PATHARRAY(1019, "_path"),
    POLYGONARRAY(1027, "_polygon"),
    CIRCLEARRAY(719, "_circle"),

    JSONARRAY(Oid.JSON_ARRAY, "_json"),
    JSONBARRAY(Oid.JSONB_ARRAY, "_jsonb"),

    CIDRARRAY(651, "_cidr"),
    INETARRAY(1041, "_inet"),
    MACADDRARRAY(1040, "_macaddr"),
    MACADDR8ARRAY(775, "_macaddr8"),

    UUIDARRAY(Oid.UUID_ARRAY, "_uuid"),
    XMLARRAY(Oid.XML_ARRAY, "_xml"),
    TSVECTORARRAY(3643, "_tsvector"),
    TSQUERYARRAY(3645, "_tsquery"),
    PG_LSNARRAY(3221, "_pg_lsn"),
    PG_SNAPSHOTARRAY(5039, "_pg_snapshot"),
    TXID_SNAPSHOTARRAY(2949, "_txid_snapshot"),

    NUMRANGEARRAY(3907, "_numrange"),
    INT4RANGEARRAY(3905, "_int4range"),
    INT8RANGEARRAY(3927, "_int8range"),
    TSRANGEARRAY(3909, "_tsrange"),
    TSTZRANGEARRAY(3911, "_tstzrange"),
    DATERANGEARRAY(3913, "_daterange"),
    INT4MULTIRANGEARRAY(6150, "_int4multirange"),
    INT8MULTIRANGEARRAY(6157, "_int8multirange"),
    NUMMULTIRANGEARRAY(6151, "_nummultirange"),
    DATEMULTIRANGEARRAY(6155, "_datemultirange"),
    TSMULTIRANGEARRAY(6152, "_tsmultirange"),
    TSTZMULTIRANGEARRAY(6153, "_tstzmultirange"),

    ENUM(-1,"enum"),
    UNKNOWN(-2,"unknown");

    public static final int ENUM_TYPE_ID = -1;
    public static final int UNKNOWN_TYPE_ID = -2;
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
