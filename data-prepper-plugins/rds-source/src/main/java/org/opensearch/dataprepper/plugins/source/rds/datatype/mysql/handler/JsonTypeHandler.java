package org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.handler;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;

public class JsonTypeHandler implements MySQLDataTypeHandler {

    @Override
    public String handle(final MySQLDataType columnType, final String columnName, final Object value,
                         final TableMetadata metadata) {
        return convertToJson(value);
    }

    private String convertToJson(final Object value) {
        try {
            if (value instanceof byte[]) {
                return JsonBinary.parseAsString((byte[])value);
            } else {
                return value.toString();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
