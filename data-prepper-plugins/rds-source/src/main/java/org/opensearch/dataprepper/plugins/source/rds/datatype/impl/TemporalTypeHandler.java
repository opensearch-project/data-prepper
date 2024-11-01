package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;

public class TemporalTypeHandler implements DataTypeHandler {

    @Override
    public String handle(final MySQLDataType columnType, final String columnName, final Object value,
                         final TableMetadata metadata) {
        // Date and Time types
        switch (columnType) {
            // TODO: Implement the transformation
            case DATE:
            case TIME:
            case TIMESTAMP:
            case DATETIME:
            case YEAR:
                return value.toString();
            default:
                throw new IllegalArgumentException("Unsupported temporal data type: " + columnType);
        }
    }
}
