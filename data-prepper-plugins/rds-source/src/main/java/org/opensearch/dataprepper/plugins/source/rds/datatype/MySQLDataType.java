package org.opensearch.dataprepper.plugins.source.rds.datatype;

import java.util.HashMap;
import java.util.Map;

public enum MySQLDataType {
    // Numeric types
    TINYINT("tinyint", DataCategory.NUMERIC, DataSubCategory.SIGNED),
    TINYINT_UNSIGNED("tinyint unsigned", DataCategory.NUMERIC, DataSubCategory.UNSIGNED),
    SMALLINT("smallint", DataCategory.NUMERIC, DataSubCategory.SIGNED),
    SMALLINT_UNSIGNED("smallint unsigned", DataCategory.NUMERIC, DataSubCategory.UNSIGNED),
    MEDIUMINT("mediumint", DataCategory.NUMERIC, DataSubCategory.SIGNED),
    MEDIUMINT_UNSIGNED("mediumint unsigned", DataCategory.NUMERIC, DataSubCategory.UNSIGNED),
    INT("int", DataCategory.NUMERIC, DataSubCategory.SIGNED),
    INT_UNSIGNED("int unsigned", DataCategory.NUMERIC, DataSubCategory.UNSIGNED),
    BIGINT("bigint", DataCategory.NUMERIC, DataSubCategory.SIGNED),
    BIGINT_UNSIGNED("bigint unsigned", DataCategory.NUMERIC, DataSubCategory.UNSIGNED),
    DECIMAL("decimal", DataCategory.NUMERIC, DataSubCategory.SIGNED),
    FLOAT("float", DataCategory.NUMERIC, DataSubCategory.SIGNED),
    DOUBLE("double", DataCategory.NUMERIC, DataSubCategory.SIGNED),
    BIT("bit", DataCategory.NUMERIC, DataSubCategory.BIT),

    // String types
    CHAR("char", DataCategory.STRING, DataSubCategory.CHAR),
    VARCHAR("varchar", DataCategory.STRING, DataSubCategory.CHAR),
    TINYTEXT("tinytext", DataCategory.STRING, DataSubCategory.BYTES),
    TEXT("text", DataCategory.STRING, DataSubCategory.BYTES),
    MEDIUMTEXT("mediumtext", DataCategory.STRING, DataSubCategory.BYTES),
    LONGTEXT("longtext", DataCategory.STRING, DataSubCategory.BYTES),
    ENUM("enum", DataCategory.STRING, DataSubCategory.ENUM),
    SET("set", DataCategory.STRING, DataSubCategory.SET),

    // Date and time types
    DATE("date", DataCategory.TEMPORAL),
    TIME("time", DataCategory.TEMPORAL),
    DATETIME("datetime", DataCategory.TEMPORAL),
    TIMESTAMP("timestamp", DataCategory.TEMPORAL),
    YEAR("year", DataCategory.TEMPORAL),

    // Binary types
    BINARY("binary", DataCategory.BINARY),
    VARBINARY("varbinary", DataCategory.BINARY),
    TINYBLOB("tinyblob", DataCategory.BINARY),
    BLOB("blob", DataCategory.BINARY),
    MEDIUMBLOB("mediumblob", DataCategory.BINARY),
    LONGBLOB("longblob", DataCategory.BINARY),

    // Special types
    JSON("json", DataCategory.JSON),
    GEOMETRY("geometry", DataCategory.SPATIAL);

    private static final Map<String, MySQLDataType> TYPE_MAP;

    static {
        TYPE_MAP = new HashMap<>(values().length);
        for (MySQLDataType dataType : values()) {
            TYPE_MAP.put(dataType.dataType, dataType);
        }
    }

    private final String dataType;
    private final DataCategory category;
    private final DataSubCategory subCategory;

    MySQLDataType(String dataType, DataCategory category) {
        this.dataType = dataType;
        this.category = category;
        this.subCategory = null;
    }

    MySQLDataType(String dataType, DataCategory category, DataSubCategory subCategory) {
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

    public static MySQLDataType byDataType(final String dataType) {
        final MySQLDataType type = TYPE_MAP.get(dataType.toLowerCase());
        if (type == null) {
            throw new IllegalArgumentException("Unsupported MySQL data type: " + dataType);
        }
        return type;
    }

    public enum DataCategory {
        NUMERIC,
        STRING,
        TEMPORAL,
        BINARY,
        JSON,
        SPATIAL
    }

    public enum DataSubCategory {
        BIT,
        SIGNED,
        UNSIGNED,
        CHAR,
        BYTES,
        TEMPORAL,
        BINARY,
        JSON,
        SPATIAL,
        ENUM,
        SET
    }

    public boolean isNumeric() {
        return category == DataCategory.NUMERIC;
    }

    public boolean isUnsigned() {
        return category == DataCategory.NUMERIC && subCategory == DataSubCategory.UNSIGNED;
    }

    public boolean isString() {
        return category == DataCategory.STRING;
    }

    public boolean isStringBytes() {
        return category == DataCategory.STRING && subCategory == DataSubCategory.BYTES;
    }

    public boolean isStringSet() {
        return category == DataCategory.STRING && subCategory == DataSubCategory.SET;
    }

    public boolean isStringEnum() {
        return category == DataCategory.STRING && subCategory == DataSubCategory.ENUM;
    }

    public boolean isTemporal() {
        return category == DataCategory.TEMPORAL;
    }

    public boolean isBinary() {
        return category == DataCategory.BINARY;
    }

    public boolean isJson() {
        return category == DataCategory.JSON;
    }

    public boolean isSpatial() {
        return category == DataCategory.SPATIAL;
    }

    public long getUnsignedMask() {
        switch (this) {
            case TINYINT_UNSIGNED:
                return 0xFFL;
            case SMALLINT_UNSIGNED:
                return 0xFFFFL;
            case MEDIUMINT_UNSIGNED:
                return 0xFFFFFFL;
            case INT_UNSIGNED:
                return 0xFFFFFFFFL;
            default:
                throw new UnsupportedOperationException("No mask for non-unsigned type: " + this);
        }
    }
}
