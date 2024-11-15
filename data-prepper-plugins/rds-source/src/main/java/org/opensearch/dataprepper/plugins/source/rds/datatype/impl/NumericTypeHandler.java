package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Map;

public class NumericTypeHandler implements DataTypeHandler {

    @Override
    public Number handle(final MySQLDataType columnType, final String columnName, final Object value,
                         final TableMetadata metadata) {
        if (value == null) {
            return null;
        }

        if (!(columnType.isNumeric() || columnType.isBit())) {
            throw new IllegalArgumentException("ColumnType is not numeric: " + columnType);
        }

        return handleNumericType(columnType, value);
    }

    private Number handleNumericType(final MySQLDataType columnType, final Object value) {
        if (columnType.isNumericUnsigned()) {
            if (columnType.isBigIntUnsigned()) {
                return handleUnsignedBigInt(value);
            } else {
                return handleUnsignedNumber(value, columnType.getUnsignedMask());
            }
        }

        if (columnType.isBit()) {
            return handleBit(value);
        }

        if (value instanceof Number) {
            return (Number)value;
        }

        throw new IllegalArgumentException("Unsupported value type. The value is of type: " + value.getClass());
    }

    private Number handleBit(final Object value) {
        if (value instanceof BitSet) {
            return bitSetToBigInteger((BitSet) value);
        }

        if (value instanceof Map) {
            Object data = ((Map<?, ?>)value).get(BYTES_KEY);
            if (data instanceof byte[]) {
                return new BigInteger(1, (byte[]) data);
            } else {
                byte[] bytes = ((String)data).getBytes();
                return new BigInteger(1, bytes);
            }
        }

        throw new IllegalArgumentException("Unsupported value type. The value is of type: " + value.getClass());
    }

    private Number handleUnsignedNumber(final Object value, final long mask) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("Unsupported value type. The value is of type: " + value.getClass());
        }

        final long longVal = ((Number)value).longValue();
        return longVal < 0 ? longVal & mask : longVal;
    }

    private Number handleUnsignedBigInt(final Object value) {
        if (value instanceof Number) {
            long longVal = ((Number)value).longValue();
            if (longVal < 0) {
                return BigInteger.valueOf(longVal & Long.MAX_VALUE)
                        .add(BigInteger.valueOf(Long.MAX_VALUE))
                        .add(BigInteger.ONE);
            }
            return (Number)value;
        }

        if (value instanceof ArrayList<?>) {
            ArrayList<?> list = (ArrayList<?>) value;

            // Convert ArrayList to byte array
            byte[] bytes = new byte[list.size()];
            for (int i = 0; i < list.size(); i++) {
                bytes[i] = ((Number) list.get(i)).byteValue();
            }

            return new BigInteger(1, bytes);
        }

        throw new IllegalArgumentException("Unsupported value type. The value is of type: " + value.getClass().getName());
    }

    private static BigInteger bitSetToBigInteger(BitSet bitSet) {
        BigInteger result = BigInteger.ZERO;
        for (int i = 0; i < bitSet.length(); i++) {
            if (bitSet.get(i)) {
                result = result.setBit(i);
            }
        }
        return result;
    }
}
