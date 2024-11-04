package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;

import java.util.Base64;

public class BinaryTypeHandler implements DataTypeHandler {

    @Override
    public String handle(final MySQLDataType columnType, final String columnName, final Object value,
                         final TableMetadata metadata) {
        return Base64.getEncoder().encodeToString((byte[]) value);
    }
}
