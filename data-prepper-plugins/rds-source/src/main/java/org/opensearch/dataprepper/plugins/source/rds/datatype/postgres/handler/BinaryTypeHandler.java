package org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.handler;

import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.utils.PgArrayParser;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.rds.utils.BytesHexConverter.bytesToHex;


public class BinaryTypeHandler implements PostgresDataTypeHandler {
    @Override
    public Object handle(PostgresDataType columnType, String columnName, Object value) {
        if (!columnType.isBinary()) {
            throw new IllegalArgumentException("ColumnType is not Binary : " + columnType);
        }
       if(columnType.isSubCategoryArray())
            return PgArrayParser.parseTypedArray(value.toString(), PostgresDataType.getScalarType(columnType),
                    this::parseBinaryValue);
       return parseBinaryValue(columnType, value);
    }

    private Object parseBinaryValue(PostgresDataType columnType, Object value) {
        if (value instanceof Map) {
            Object data = ((Map<?, ?>)value).get(BYTES_KEY);
            byte[] bytes = ((String) data).getBytes(StandardCharsets.ISO_8859_1);
            return "\\x" + bytesToHex(bytes);
        }
        return value.toString();
    }
}
