package org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

import java.util.Map;

public class BinaryTypeHandler implements MySQLDataTypeHandler {
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
