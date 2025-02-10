package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;

import java.util.Objects;

public class BooleanTypeHandler implements PostgresDataTypeHandler {
    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isBoolean()) {
            throw new IllegalArgumentException("ColumnType is not Boolean: " + columnType);
        }
        return (Objects.equals(value.toString(), "t")) ? Boolean.TRUE: Boolean.FALSE;
    }
}
