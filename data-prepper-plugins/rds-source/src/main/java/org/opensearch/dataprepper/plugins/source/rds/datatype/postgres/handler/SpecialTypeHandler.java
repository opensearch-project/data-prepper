package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.utils.PgArrayParser;


public class SpecialTypeHandler implements PostgresDataTypeHandler {
    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isSpecial()) {
            throw new IllegalArgumentException("ColumnType is not special: " + columnType);
        }
        if (columnType.isSubCategoryArray())
            return PgArrayParser.parseTypedArray(value.toString(), PostgresDataType.getScalarType(columnType),
                    this::parseSpecialValue);
        return parseSpecialValue(columnType, value.toString());
    }

    private Object parseSpecialValue(PostgresDataType columnType, String textValue) {
        return textValue;
    }

}
