package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres;

import java.util.HashMap;
import java.util.Map;

public enum PostgresDataType {
    // Numeric types
    SMALLINT("int2", DataCategory.NUMERIC),
    INTEGER("int4", DataCategory.NUMERIC),
    BIGINT("int8", DataCategory.NUMERIC),
    SMALLSERIAL("smallserial", DataCategory.NUMERIC),
    SERIAL("serial", DataCategory.NUMERIC),
    BIGSERIAL("bigserial", DataCategory.NUMERIC),
    REAL("float4", DataCategory.NUMERIC),
    DOUBLE_PRECISION("float8", DataCategory.NUMERIC),
    NUMERIC("numeric", DataCategory.NUMERIC),
    MONEY("money", DataCategory.NUMERIC),

    //String Data types
    TEXT("text", DataCategory.STRING),
    VARCHAR("varchar", DataCategory.STRING),
    BPCHAR("bpchar", DataCategory.STRING),
    ENUM("enum", DataCategory.STRING),

    //Bit String Data type
    BIT("bit",DataCategory.BIT_STRING),
    VARBIT("varbit", DataCategory.BIT_STRING),

    //Json Data type
    JSON("json",DataCategory.JSON),
    JSONB("jsonb",DataCategory.JSON),
    JSONPATH("jsonpath", DataCategory.JSON),

    //Boolean data type
    BOOLEAN("bool", DataCategory.BOOLEAN),

    //Date-time data types
    DATE("date", DataCategory.TEMPORAL),
    TIME("time",DataCategory.TEMPORAL),
    TIMETZ("timetz",DataCategory.TEMPORAL),
    TIMESTAMP("timestamp",DataCategory.TEMPORAL),
    TIMESTAMPTZ("timestamptz",DataCategory.TEMPORAL),
    INTERVAL("interval", DataCategory.TEMPORAL),

    //Spatial Data types
    POINT("point", DataCategory.SPATIAL),
    LINE("line", DataCategory.SPATIAL),
    LSEG("lseg", DataCategory.SPATIAL),
    BOX("box", DataCategory.SPATIAL),
    PATH("path", DataCategory.SPATIAL),
    POLYGON("polygon", DataCategory.SPATIAL),
    CIRCLE("circle", DataCategory.SPATIAL),

    //Network Address Data types
    CIDR("cidr", DataCategory.NETWORK_ADDRESS),
    INET("inet", DataCategory.NETWORK_ADDRESS),
    MACADDR("macaddr", DataCategory.NETWORK_ADDRESS),
    MACADDR8("macaddr8", DataCategory.NETWORK_ADDRESS),

    //Special Data types
    UUID( "uuid",DataCategory.SPECIAL),
    XML("xml", DataCategory.SPECIAL),
    PG_LSN("pg_lsn", DataCategory.SPECIAL),
    TSVECTOR("tsvector", DataCategory.SPECIAL),
    TSQUERY("tsquery", DataCategory.SPECIAL),
    PG_SNAPSHOT("pg_snapshot", DataCategory.SPECIAL),
    TXID_SNAPSHOT("txid_snapshot", DataCategory.SPECIAL),

    INT4RANGE("int4range", DataCategory.RANGE),
    INT8RANGE("int8range", DataCategory.RANGE),
    TSRANGE("tsrange", DataCategory.RANGE),
    TSTZRANGE("tstzrange", DataCategory.RANGE),
    DATERANGE("daterange", DataCategory.RANGE),

    //Binary data type
    BYTEA("bytea", DataCategory.BINARY);

    private static final Map<String, PostgresDataType> TYPE_MAP;

    static {
        TYPE_MAP = new HashMap<>(values().length);
        for (PostgresDataType dataType : values()) {
            TYPE_MAP.put(dataType.dataType, dataType);
        }
    }

    private final String dataType;
    private final DataCategory category;

    PostgresDataType(String dataType, DataCategory category) {
        this.dataType = dataType;
        this.category = category;
    }

    public String getDataType() {
        return dataType;
    }

    public DataCategory getCategory() {
        return category;
    }


    public static PostgresDataType byDataType(final String dataType) {
        final PostgresDataType type = TYPE_MAP.get(dataType.toLowerCase());
        if (type == null) {
            throw new IllegalArgumentException("Unsupported PostgresDataType data type: " + dataType);
        }
        return type;
    }

    public enum DataCategory {
        NUMERIC,
        STRING,
        BIT_STRING,
        JSON,
        BOOLEAN,
        TEMPORAL,
        SPATIAL,
        NETWORK_ADDRESS,
        SPECIAL,
        BINARY,
        RANGE
    }


    public boolean isNumeric() {
        return category == DataCategory.NUMERIC;
    }


    public boolean isString() {
        return category == DataCategory.STRING;
    }

    public boolean isBitString() {
        return category == DataCategory.BIT_STRING;
    }

    public boolean isJson() {
        return category == DataCategory.JSON;
    }

    public boolean isBoolean() {
        return category == DataCategory.BOOLEAN;
    }

    public boolean isTemporal() {
        return category == DataCategory.TEMPORAL;
    }

    public boolean isSpatial() {
        return category == DataCategory.SPATIAL;
    }

    public boolean isNetworkAddress() {
        return category == DataCategory.NETWORK_ADDRESS;
    }

    public boolean isSpecial() {
        return category == DataCategory.SPECIAL;
    }

    public boolean isBinary() {
        return category == DataCategory.BINARY;
    }

    public boolean isRange() {
        return category == DataCategory.RANGE;
    }

}

