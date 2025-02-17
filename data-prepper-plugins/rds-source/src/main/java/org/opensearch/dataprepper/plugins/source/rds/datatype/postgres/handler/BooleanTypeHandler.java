package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;

public class BooleanTypeHandler implements PostgresDataTypeHandler {
    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isBoolean()) {
            throw new IllegalArgumentException("ColumnType is not Boolean: " + columnType);
        }
        final String booleanValue = value.toString();
        return booleanValue.equals("t") || booleanValue.equals("true") ? Boolean.TRUE : Boolean.FALSE;
    }
}
