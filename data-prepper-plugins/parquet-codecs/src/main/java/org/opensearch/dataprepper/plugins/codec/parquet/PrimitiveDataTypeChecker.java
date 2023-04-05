package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class PrimitiveDataTypeChecker {

    public static Object checkPrimitiveDataType(Type field, SimpleGroup simpleGroup, int fieldIndex) {
        String fieldPrimitiveDataType;
        if (field.isPrimitive()) {
            PrimitiveType primitiveType = field.asPrimitiveType();
            fieldPrimitiveDataType = primitiveType.getPrimitiveTypeName().name();
            switch (fieldPrimitiveDataType) {
                case "INT64":
                case "INT32":
                    return simpleGroup.getInteger(fieldIndex, 0);
                case "BOOLEAN":
                    return simpleGroup.getBoolean(fieldIndex, 0);
                case "FLOAT":
                    return simpleGroup.getFloat(fieldIndex, 0);
                case "DOUBLE":
                    return simpleGroup.getDouble(fieldIndex, 0);
                case "INT96":
                    return simpleGroup.getInt96(fieldIndex, 0);
                case "FIXED_LEN_BYTE_ARRAY":
                    return simpleGroup.getValueToString(fieldIndex, 0);
                default:
                    //
            }
        }
        return simpleGroup.getValueToString(fieldIndex, 0);
    }
}
