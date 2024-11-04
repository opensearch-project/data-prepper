package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;

public class SpatialTypeHandler implements DataTypeHandler {

    @Override
    public String handle(final MySQLDataType columnType, final String columnName, final Object value,
                         final TableMetadata metadata) {
        // TODO: Implement the transformation
        return new String((byte[]) value);
    }
}
