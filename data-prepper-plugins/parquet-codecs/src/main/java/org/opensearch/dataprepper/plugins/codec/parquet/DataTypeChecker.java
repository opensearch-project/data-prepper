/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.parquet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.PrimitiveType;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.util.UUID;

public class DataTypeChecker {

    private DataTypeChecker(){

    }

    public static Object checkDataType(Type field, SimpleGroup simpleGroup, int fieldIndex) throws Exception {
        String fieldPrimitiveDataType;
        if (field.isPrimitive()) {
            PrimitiveType primitiveType = field.asPrimitiveType();
            fieldPrimitiveDataType = primitiveType.getPrimitiveTypeName().name();
            switch (fieldPrimitiveDataType) {
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
                case "INT64":
                case "LONG":
                    return simpleGroup.getLong(fieldIndex,0);
                case "FIXED_LEN_BYTE_ARRAY":
                    return simpleGroup.getValueToString(fieldIndex, 0);
                default:
                    throw new RuntimeException("The Parquet Codec doesn't support this data-type yet.");
            }
        }
        else if (field.getLogicalTypeAnnotation() != null) {
                if (field.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.dateType())) {

                    return simpleGroup.getInteger(fieldIndex, 0);
                }
                else if (field.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))) {

                    return simpleGroup.getLong(fieldIndex, 0);

                }
                else if (field.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.intType(8,true))
                || field.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.intType(16,true))
                || field.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.intType(32,true))
                || field.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.intType(64,true))
                || field.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.intType(8,false))
                        || field.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.intType(16,false))
                        || field.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.intType(32,false))
                        || field.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.intType(64,false))){

                    return simpleGroup.getInteger(fieldIndex,0);
                }
                else if (field.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.stringType() )) {

                   return simpleGroup.getValueToString(fieldIndex,0);
                }
                else if (field.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.uuidType())){

                    byte[] uuidBytes = simpleGroup.getBinary(fieldIndex,0).getBytes();
                    return UUID.fromString(new String(uuidBytes));
                }
                else if (field.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.jsonType())){

                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode jsonNode = mapper.readTree(simpleGroup.getBinary(fieldIndex,0).getBytes());
                    return jsonNode.get(field.getName().toString());
                }
                else{
                    throw new RuntimeException("The Parquet Codec doesn't support this logical data-type yet.");
                }
            }
        else {
            throw new RuntimeException("The Parquet Codec doesn't support this data-type yet.");
        }
    }
}
