package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;

import java.util.Map;

public class BinaryTypeHandler implements DataTypeHandler {
    @Override
    public String handle(final MySQLDataType columnType, final String columnName, final Object value,
                         final TableMetadata metadata) {
        if (value instanceof byte[]) {
            return new String((byte[]) value);
        }

        if (value instanceof Map) {
            Object data = ((Map<?, ?>)value).get(BYTES_KEY);
            if (data instanceof byte[]) {
                return new String((byte[]) data);
            } else {
                return data.toString();
            }
        }

        return value.toString();
    }
}
