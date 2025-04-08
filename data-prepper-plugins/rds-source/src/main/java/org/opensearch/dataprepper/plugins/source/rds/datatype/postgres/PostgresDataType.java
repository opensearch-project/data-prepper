package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum PostgresDataType {
    // Numeric types
    SMALLINT("int2", DataCategory.NUMERIC, DataSubCategory.SCALAR),
    INTEGER("int4", DataCategory.NUMERIC, DataSubCategory.SCALAR),
    BIGINT("int8", DataCategory.NUMERIC, DataSubCategory.SCALAR),
    SMALLSERIAL("smallserial", DataCategory.NUMERIC, DataSubCategory.SCALAR),
    SERIAL("serial", DataCategory.NUMERIC, DataSubCategory.SCALAR),
    BIGSERIAL("bigserial", DataCategory.NUMERIC, DataSubCategory.SCALAR),
    REAL("float4", DataCategory.NUMERIC, DataSubCategory.SCALAR),
    DOUBLE_PRECISION("float8", DataCategory.NUMERIC, DataSubCategory.SCALAR),
    NUMERIC("numeric", DataCategory.NUMERIC, DataSubCategory.SCALAR),
    MONEY("money", DataCategory.NUMERIC, DataSubCategory.SCALAR),
    INT2ARRAY("_int2", DataCategory.NUMERIC, DataSubCategory.ARRAY),
    INT4ARRAY("_int4", DataCategory.NUMERIC, DataSubCategory.ARRAY),
    INT8ARRAY("_int8", DataCategory.NUMERIC, DataSubCategory.ARRAY),
    FLOAT4ARRAY("_float4", DataCategory.NUMERIC, DataSubCategory.ARRAY),
    FLOAT8ARRAY("_float8", DataCategory.NUMERIC, DataSubCategory.ARRAY),
    NUMERICARRAY("_numeric", DataCategory.NUMERIC, DataSubCategory.ARRAY),
    MONEYARRAY("_money", DataCategory.NUMERIC, DataSubCategory.ARRAY),

    //String Data types
    TEXT("text", DataCategory.STRING, DataSubCategory.SCALAR),
    VARCHAR("varchar", DataCategory.STRING, DataSubCategory.SCALAR),
    BPCHAR("bpchar", DataCategory.STRING, DataSubCategory.SCALAR),
    TEXTARRAY("_text", DataCategory.STRING, DataSubCategory.ARRAY),
    VARCHARARRAY("_varchar", DataCategory.STRING, DataSubCategory.ARRAY),
    BPCHARARRAY("_bpchar", DataCategory.STRING, DataSubCategory.ARRAY),
    ENUM("enum", DataCategory.STRING),

    //Bit String Data type
    BIT("bit",DataCategory.BIT_STRING, DataSubCategory.SCALAR),
    VARBIT("varbit", DataCategory.BIT_STRING, DataSubCategory.SCALAR),
    BITARRAY("_bit", DataCategory.BIT_STRING, DataSubCategory.ARRAY),
    VARBITARRAY("_varbit", DataCategory.BIT_STRING, DataSubCategory.ARRAY),

    //Json Data type
    JSON("json",DataCategory.JSON, DataSubCategory.SCALAR),
    JSONB("jsonb",DataCategory.JSON, DataSubCategory.SCALAR),
    JSONPATH("jsonpath", DataCategory.JSON, DataSubCategory.SCALAR),
    JSONARRAY("_json", DataCategory.JSON, DataSubCategory.ARRAY),
    JSONBARRAY("_jsonb", DataCategory.JSON, DataSubCategory.ARRAY),

    //Boolean data type
    BOOLEAN("bool", DataCategory.BOOLEAN, DataSubCategory.SCALAR),
    BOOLEANARRAY("_bool", DataCategory.BOOLEAN, DataSubCategory.ARRAY),

    //Date-time data types
    DATE("date", DataCategory.TEMPORAL, DataSubCategory.SCALAR),
    TIME("time",DataCategory.TEMPORAL, DataSubCategory.SCALAR),
    TIMETZ("timetz",DataCategory.TEMPORAL, DataSubCategory.SCALAR),
    TIMESTAMP("timestamp",DataCategory.TEMPORAL, DataSubCategory.SCALAR),
    TIMESTAMPTZ("timestamptz",DataCategory.TEMPORAL, DataSubCategory.SCALAR),
    INTERVAL("interval", DataCategory.TEMPORAL, DataSubCategory.SCALAR),
    DATEARRAY("_date", DataCategory.TEMPORAL, DataSubCategory.ARRAY),
    TIMEARRAY("_time", DataCategory.TEMPORAL, DataSubCategory.ARRAY),
    TIMETZARRAY("_timetz", DataCategory.TEMPORAL, DataSubCategory.ARRAY),
    TIMESTAMPARRAY("_timestamp", DataCategory.TEMPORAL, DataSubCategory.ARRAY),
    TIMESTAMPTZARRAY("_timestamptz", DataCategory.TEMPORAL, DataSubCategory.ARRAY),
    INTERVALARRAY("_interval", DataCategory.TEMPORAL, DataSubCategory.ARRAY),

    //Spatial Data types
    POINT("point", DataCategory.SPATIAL, DataSubCategory.SCALAR),
    LINE("line", DataCategory.SPATIAL, DataSubCategory.SCALAR),
    LSEG("lseg", DataCategory.SPATIAL, DataSubCategory.SCALAR),
    BOX("box", DataCategory.SPATIAL, DataSubCategory.SCALAR),
    PATH("path", DataCategory.SPATIAL, DataSubCategory.SCALAR),
    POLYGON("polygon", DataCategory.SPATIAL, DataSubCategory.SCALAR),
    CIRCLE("circle", DataCategory.SPATIAL, DataSubCategory.SCALAR),
    POINTARRAY("_point", DataCategory.SPATIAL, DataSubCategory.ARRAY),
    LINEARRAY("_line", DataCategory.SPATIAL, DataSubCategory.ARRAY),
    LSEGARRAY("_lseg", DataCategory.SPATIAL, DataSubCategory.ARRAY),
    BOXARRAY("_box", DataCategory.SPATIAL, DataSubCategory.ARRAY),
    PATHARRAY("_path", DataCategory.SPATIAL, DataSubCategory.ARRAY),
    POLYGONARRAY("_polygon", DataCategory.SPATIAL, DataSubCategory.ARRAY),
    CIRCLEARRAY("_circle", DataCategory.SPATIAL, DataSubCategory.ARRAY),

    //Network Address Data types
    CIDR("cidr", DataCategory.NETWORK_ADDRESS, DataSubCategory.SCALAR),
    INET("inet", DataCategory.NETWORK_ADDRESS, DataSubCategory.SCALAR),
    MACADDR("macaddr", DataCategory.NETWORK_ADDRESS, DataSubCategory.SCALAR),
    MACADDR8("macaddr8", DataCategory.NETWORK_ADDRESS, DataSubCategory.SCALAR),
    CIDRARRAY("_cidr", DataCategory.NETWORK_ADDRESS, DataSubCategory.ARRAY),
    INETARRAY("_inet", DataCategory.NETWORK_ADDRESS, DataSubCategory.ARRAY),
    MACADDRARRAY("_macaddr", DataCategory.NETWORK_ADDRESS, DataSubCategory.ARRAY),
    MACADDR8ARRAY("_macaddr8", DataCategory.NETWORK_ADDRESS, DataSubCategory.ARRAY),

    //Special Data types
    UUID( "uuid",DataCategory.SPECIAL, DataSubCategory.SCALAR),
    XML("xml", DataCategory.SPECIAL, DataSubCategory.SCALAR),
    PG_LSN("pg_lsn", DataCategory.SPECIAL, DataSubCategory.SCALAR),
    TSVECTOR("tsvector", DataCategory.SPECIAL, DataSubCategory.SCALAR),
    TSQUERY("tsquery", DataCategory.SPECIAL, DataSubCategory.SCALAR),
    PG_SNAPSHOT("pg_snapshot", DataCategory.SPECIAL, DataSubCategory.SCALAR),
    TXID_SNAPSHOT("txid_snapshot", DataCategory.SPECIAL, DataSubCategory.SCALAR),
    UUIDARRAY("_uuid", DataCategory.SPECIAL, DataSubCategory.ARRAY),
    XMLARRAY("_xml", DataCategory.SPECIAL, DataSubCategory.ARRAY),
    PG_LSNARRAY("_pg_lsn", DataCategory.SPECIAL, DataSubCategory.ARRAY),
    TSVECTORARRAY("_tsvector", DataCategory.SPECIAL, DataSubCategory.ARRAY),
    TSQUERYARRAY("_tsquery", DataCategory.SPECIAL, DataSubCategory.ARRAY),
    PG_SNAPSHOTARRAY("_pg_snapshot", DataCategory.SPECIAL, DataSubCategory.ARRAY),
    TXID_SNAPSHOTARRAY("_txid_snapshot", DataCategory.SPECIAL, DataSubCategory.ARRAY),
    UNKNOWN("unknown", DataCategory.SPECIAL),

    INT4RANGE("int4range", DataCategory.RANGE, DataSubCategory.SCALAR),
    INT8RANGE("int8range", DataCategory.RANGE, DataSubCategory.SCALAR),
    NUMRANGE("numrange", DataCategory.RANGE, DataSubCategory.SCALAR),
    TSRANGE("tsrange", DataCategory.RANGE, DataSubCategory.SCALAR),
    TSTZRANGE("tstzrange", DataCategory.RANGE, DataSubCategory.SCALAR),
    DATERANGE("daterange", DataCategory.RANGE, DataSubCategory.SCALAR),
    INT4MULTIRANGE("int4multirange", DataCategory.RANGE, DataSubCategory.SCALAR),
    INT8MULTIRANGE("int8multirange", DataCategory.RANGE, DataSubCategory.SCALAR),
    NUMMULTIRANGE("nummultirange", DataCategory.RANGE, DataSubCategory.SCALAR),
    DATEMULTIRANGE("datemultirange", DataCategory.RANGE, DataSubCategory.SCALAR),
    TSMULTIRANGE("tsmultirange", DataCategory.RANGE, DataSubCategory.SCALAR),
    TSTZMULTIRANGE("tstzmultirange", DataCategory.RANGE, DataSubCategory.SCALAR),
    NUMRANGEARRAY( "_numrange", DataCategory.RANGE, DataSubCategory.ARRAY),
    INT4RANGEARRAY("_int4range", DataCategory.RANGE, DataSubCategory.ARRAY),
    INT8RANGEARRAY("_int8range", DataCategory.RANGE, DataSubCategory.ARRAY),
    TSRANGEARRAY("_tsrange", DataCategory.RANGE, DataSubCategory.ARRAY),
    TSTZRANGEARRAY("_tstzrange", DataCategory.RANGE, DataSubCategory.ARRAY),
    DATERANGEARRAY("_daterange", DataCategory.RANGE, DataSubCategory.ARRAY),
    INT4MULTIRANGEARRAY("_int4multirange", DataCategory.RANGE, DataSubCategory.ARRAY),
    INT8MULTIRANGEARRAY("_int8multirange", DataCategory.RANGE, DataSubCategory.ARRAY),
    NUMMULTIRANGEARRAY("_nummultirange", DataCategory.RANGE, DataSubCategory.ARRAY),
    DATEMULTIRANGEARRAY("_datemultirange", DataCategory.RANGE, DataSubCategory.ARRAY),
    TSMULTIRANGEARRAY("_tsmultirange", DataCategory.RANGE, DataSubCategory.ARRAY),
    TSTZMULTIRANGEARRAY("_tstzmultirange", DataCategory.RANGE, DataSubCategory.ARRAY),

    //Binary data type
    BYTEA("bytea", DataCategory.BINARY, DataSubCategory.SCALAR),
    BYTEAARRAY("_bytea", DataCategory.BINARY, DataSubCategory.ARRAY);

    private static final Map<String, PostgresDataType> TYPE_MAP;

    private static final Map<PostgresDataType, PostgresDataType> ARRAY_TO_SCALAR_MAP;

    static {
        ARRAY_TO_SCALAR_MAP = new HashMap<>();
        for (PostgresDataType type : PostgresDataType.values()) {
            if (type.getSubCategory() == DataSubCategory.ARRAY) {
                String scalarTypeName = type.getDataType().substring(1);
                try {
                    PostgresDataType scalarType = Arrays.stream(PostgresDataType.values())
                            .filter(t -> t.getDataType().equals(scalarTypeName))
                            .findFirst()
                            .orElseThrow(() -> new IllegalArgumentException("No matching scalar type found for " + type.name()));
                    ARRAY_TO_SCALAR_MAP.put(type, scalarType);
                } catch (IllegalArgumentException e) {
                    ARRAY_TO_SCALAR_MAP.put(type, PostgresDataType.UNKNOWN);
                }
            }
        }
    }

    static {
        TYPE_MAP = new HashMap<>(values().length);
        for (PostgresDataType dataType : values()) {
            TYPE_MAP.put(dataType.dataType, dataType);
        }
    }

    private final String dataType;
    private final DataCategory category;
    private final DataSubCategory subCategory;

    PostgresDataType(String dataType, DataCategory category) {
        this.dataType = dataType;
        this.category = category;
        this.subCategory = null;
    }

    PostgresDataType(String dataType, DataCategory category, DataSubCategory subCategory ) {
        this.dataType = dataType;
        this.category = category;
        this.subCategory = subCategory;
    }


    public String getDataType() {
        return dataType;
    }

    public DataCategory getCategory() {
        return category;
    }

    public DataSubCategory getSubCategory() {
        return subCategory;
    }


    public static PostgresDataType byDataType(final String dataType) {
        PostgresDataType type = TYPE_MAP.get(dataType.toLowerCase());
        if (type == null) {
            type = PostgresDataType.UNKNOWN;
        }
        return type;
    }

    public static PostgresDataType getScalarType(PostgresDataType arrayType) {
        return ARRAY_TO_SCALAR_MAP.get(arrayType);
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

    public enum DataSubCategory {
        SCALAR,
        ARRAY
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

    public boolean isSubCategoryArray() {
        return subCategory == DataSubCategory.ARRAY;
    }
}

