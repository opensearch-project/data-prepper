package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;

public class JsonTypeHandler implements PostgresDataTypeHandler {
    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isJson()) {
            throw new IllegalArgumentException("ColumnType is not Json: " + columnType);
        }
        return value.toString();
    }
}
