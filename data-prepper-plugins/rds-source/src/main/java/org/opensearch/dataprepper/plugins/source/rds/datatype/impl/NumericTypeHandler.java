package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;

public class NumericTypeHandler implements DataTypeHandler {

    @Override
    public String handle(final MySQLDataType columnType, final String columnName, final Object value,
                         final TableMetadata metadata) {
        return handleNumericType(columnType, value);
    }

    private String handleNumericType(final MySQLDataType columnType, final Object value) {
        if (columnType.isUnsigned()) {
            return handleUnsignedNumber((Number) value, columnType.getUnsignedMask());
        }
        return value.toString();
    }

    private String handleUnsignedNumber(final Number value, final long mask) {
        final long longVal = value.longValue();
        return String.valueOf(longVal < 0 ? longVal & mask : longVal);
    }
}
