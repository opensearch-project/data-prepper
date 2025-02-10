package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;

import java.math.BigInteger;

public class BitStringTypeHandler implements PostgresDataTypeHandler {
    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isBitString()) {
            throw new IllegalArgumentException("ColumnType is not Bit String: " + columnType);
        }
        return new BigInteger(value.toString(), 2);
    }
}
