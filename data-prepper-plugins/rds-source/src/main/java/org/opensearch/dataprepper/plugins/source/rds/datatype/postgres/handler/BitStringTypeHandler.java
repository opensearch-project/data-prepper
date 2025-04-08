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
        if (columnType.isSubCategoryArray())
            return PgArrayParser.parseTypedArray(value.toString(), PostgresDataType.getScalarType(columnType),
                    this::parseBitString);
        return parseBitString(columnType, value.toString());
    }

    private Object parseBitString(PostgresDataType columnType, String textValue) {
        if (textValue.isEmpty()) return null;
        return new BigInteger(textValue, 2);
    }

}
