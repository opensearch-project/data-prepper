package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;

public class NetworkAddressTypeHandler implements PostgresDataTypeHandler {
    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isNetworkAddress()) {
            throw new IllegalArgumentException("ColumnType is not Network Address: " + columnType);
        }
        return value.toString();
    }
}
