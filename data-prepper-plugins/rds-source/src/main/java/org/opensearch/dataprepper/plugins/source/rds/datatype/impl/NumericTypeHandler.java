package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;

import java.math.BigInteger;

public class NumericTypeHandler implements DataTypeHandler {

    @Override
    public String handle(final MySQLDataType columnType, final String columnName, final Object value,
                         final TableMetadata metadata) {
        if (value == null) {
            return null;
        }

        if (!columnType.isNumeric()) {
            throw new IllegalArgumentException("ColumnType is not numeric: " + columnType);
        }

        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("Value is not a number: " + value);
        }

        return handleNumericType(columnType, (Number) value);
    }

    private String handleNumericType(final MySQLDataType columnType, final Number value) {
        if (columnType.isUnsigned()) {
            if(columnType == MySQLDataType.BIGINT_UNSIGNED) {
                return handleUnsignedDouble(value);
            } else {
                return handleUnsignedNumber(value, columnType.getUnsignedMask());
            }
        }
        return value.toString();
    }

    private String handleUnsignedNumber(final Number value, final long mask) {
        final long longVal = value.longValue();
        return String.valueOf(longVal < 0 ? longVal & mask : longVal);
    }

    private String handleUnsignedDouble(final Number value) {
        long longVal = value.longValue();
        if (longVal < 0) {
            return BigInteger.valueOf(longVal & Long.MAX_VALUE)
                    .add(BigInteger.valueOf(Long.MAX_VALUE))
                    .add(BigInteger.ONE)
                    .toString();
        }
        return String.valueOf(longVal);
    }
}
