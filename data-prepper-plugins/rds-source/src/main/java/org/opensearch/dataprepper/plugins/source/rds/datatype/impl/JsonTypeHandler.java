package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;

import java.io.IOException;

public class JsonTypeHandler implements DataTypeHandler {

    @Override
    public String handle(final MySQLDataType columnType, final String columnName, final Object value,
                         final TableMetadata metadata) {
        return convertToJson((byte[]) value);
    }

    private String convertToJson(final byte[] jsonBytes) {
        try {
            return JsonBinary.parseAsString(jsonBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
