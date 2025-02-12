package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;

public class StringTypeHandler implements PostgresDataTypeHandler {
    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isString()) {
            throw new IllegalArgumentException("ColumnType is not string: " + columnType);
        }
        return value.toString();
    }
}
