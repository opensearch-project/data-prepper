package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.utils.PgArrayParser;

import java.math.BigInteger;

public class BitStringTypeHandler implements PostgresDataTypeHandler {
    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isBitString()) {
            throw new IllegalArgumentException("ColumnType is not Bit String: " + columnType);
        }
        if(columnType.isSubCategoryArray())
            return parseBitStringArray(columnType, value.toString());
        return parseBitString(columnType, value.toString());
    }

    private Object parseBitString(PostgresDataType columnType, String textValue) {
        if(textValue.isEmpty())
            return null;
        return new BigInteger(textValue, 2);
    }

    private Object parseBitStringArray(PostgresDataType columnType, String textValue) {
        switch (columnType) {
            case BITARRAY:
                return PgArrayParser.parseTypedArray(textValue, PostgresDataType.BIT, this::parseBitString);
            case VARBITARRAY:
                return PgArrayParser.parseTypedArray(textValue, PostgresDataType.VARBIT, this::parseBitString);
            default:
                return textValue;
        }
    }
}
