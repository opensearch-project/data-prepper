package org.opensearch.dataprepper.plugins.source.rds.datatype.impl;

import org.opensearch.dataprepper.plugins.source.rds.datatype.DataTypeHandler;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.datatype.MySQLDataType;

import java.util.ArrayList;
import java.util.List;

public class StringTypeHandler implements DataTypeHandler {

    @Override
    public String handle(final MySQLDataType columnType, final String columnName, final Object value,
                         final TableMetadata metadata) {
        if (columnType.isStringBytes()) {
            return new String((byte[]) value);
        } else if (columnType.isStringEnum() && value instanceof Integer) {
            return getEnumValue((int) value, metadata.getEnumStrValues().get(columnName));
        } else if (columnType.isStringSet() && value instanceof Long) {
            return getSetValues((long) value, metadata.getSetStrValues().get(columnName)).toString();
        } else {
            return value.toString();
        }
    }

    private static List<String> getSetValues(final long numericValue, final String[] setStrValues) {
        final List<String> setValues = new ArrayList<>();
        for (int i = 0; i < setStrValues.length; i++) {
            if ((numericValue & (1L << i)) != 0) {
                setValues.add(setStrValues[i].trim());
            }
        }

        return setValues;
    }

    private static String getEnumValue(final int numericValue, final String[] enumStrValues) {
        return enumStrValues[numericValue - 1];
    }
}
