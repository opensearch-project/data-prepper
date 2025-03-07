package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.utils.PgArrayParser;

public class NetworkAddressTypeHandler implements PostgresDataTypeHandler {
    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isNetworkAddress()) {
            throw new IllegalArgumentException("ColumnType is not Network Address: " + columnType);
        }
        if (columnType.isSubCategoryArray())
            return PgArrayParser.parseTypedArray(value.toString(), PostgresDataType.getScalarType(columnType),
                    this::parseNetworkAddressValue);
        return parseNetworkAddressValue(columnType, value.toString());
    }

    private Object parseNetworkAddressValue(PostgresDataType columnType, String textValue) {
        return textValue;
    }
}
