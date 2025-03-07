package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.utils.PgArrayParser;

public class BooleanTypeHandler implements PostgresDataTypeHandler {
    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isBoolean()) {
            throw new IllegalArgumentException("ColumnType is not Boolean: " + columnType);
        }
        if (columnType.isSubCategoryArray())
            return PgArrayParser.parseTypedArray(value.toString(), PostgresDataType.getScalarType(columnType),
                    this::parseBooleanValue);
        return parseBooleanValue(columnType, value.toString());
    }

    private Object parseBooleanValue(PostgresDataType columnType, String textValue) {
        return textValue.equals("t") || textValue.equals("true") ? Boolean.TRUE : Boolean.FALSE;
    }
}
