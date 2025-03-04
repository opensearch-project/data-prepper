package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.utils.PgArrayParser;

public class StringTypeHandler implements PostgresDataTypeHandler {
    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isString()) {
            throw new IllegalArgumentException("ColumnType is not string: " + columnType);
        }
        if(columnType.isSubCategoryArray())
            return parseStringArray(columnType, value.toString());
        return parseStringValue(columnType, value.toString());
    }

    private Object parseStringValue(PostgresDataType columnType, String textValue) {
        return textValue;
    }

    private Object parseStringArray(PostgresDataType columnType, String textValue) {
        switch (columnType) {
            case TEXTARRAY:
                return PgArrayParser.parseTypedArray(textValue, PostgresDataType.TEXT, this::parseStringValue);
            case VARCHARARRAY:
                return PgArrayParser.parseTypedArray(textValue, PostgresDataType.VARCHAR, this::parseStringValue);
            case BPCHARARRAY:
                return PgArrayParser.parseTypedArray(textValue, PostgresDataType.BPCHAR, this::parseStringValue);
            default:
                return textValue;
        }
    }
}