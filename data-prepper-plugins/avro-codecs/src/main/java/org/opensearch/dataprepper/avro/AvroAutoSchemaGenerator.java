package org.opensearch.dataprepper.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.util.List;
import java.util.Map;

/**
 * Generates an Avro schema from Event data.
 */
public class AvroAutoSchemaGenerator {
    public Schema autoDetermineSchema(final Map<String, Object> data,
                                      final OutputCodecContext outputCodecContext) {
        return autoGenerateRecordSchema(data, outputCodecContext, "Event");
    }

    private Schema autoGenerateRecordSchema(final Map<String, Object> eventData, OutputCodecContext codecContext, String typeName) {
        SchemaBuilder.FieldAssembler<Schema> fieldsAssembler = SchemaBuilder.record(typeName).fields();
        for (final String key : eventData.keySet()) {
            if (codecContext != null && codecContext.getExcludeKeys().contains(key)) {
                continue;
            }
            Schema schemaForValue = createSchemaForValue(key, eventData.get(key), codecContext);
            fieldsAssembler = fieldsAssembler.name(key).type().unionOf()
                    .nullType()
                    .and()
                    .type(schemaForValue)
                    .endUnion()
                    .nullDefault();
        }
        return fieldsAssembler.endRecord();
    }


    private Schema createSchemaForValue(String key, final Object value, OutputCodecContext codecContext) {
        if(value == null)
            throw new SchemaGenerationException("Unable to auto-generate a schema because a provided value is null. key='" + key + "'.");
        if (value instanceof String) {
            return SchemaBuilder.builder().stringType();
        } else if (value instanceof Long) {
            return SchemaBuilder.builder().longType();
        } else if (value instanceof Integer) {
            return SchemaBuilder.builder().intType();
        } else if (value instanceof Float) {
            return SchemaBuilder.builder().floatType();
        } else if (value instanceof Double) {
            return SchemaBuilder.builder().doubleType();
        } else if (value instanceof Boolean) {
            return SchemaBuilder.builder().booleanType();
        } else if (value instanceof Byte[] || value instanceof byte[]) {
            return SchemaBuilder.builder().bytesType();
        } else if (value instanceof Map) {
            return autoGenerateRecordSchema((Map<String, Object>) value, codecContext, convertFieldNameToTypeName(key));
        } else if (value instanceof List) {
            List<?> listProvided = (List<?>) value;
            if(listProvided.isEmpty()) {
                throw new SchemaGenerationException("Cannot determine the element type for the Avro array because a provided list is empty and has no type information. key='" + key + "'.");
            }
            Object sampleElement = listProvided.get(0);
            return SchemaBuilder.builder()
                    .array()
                    .items(nullableType(createSchemaForValue(null, sampleElement, codecContext)));
        }
        throw new SchemaGenerationException("Unable to auto-generate a schema for field '" +
                key +
                "' because the the type '" + value.getClass() + "' is not a recognized type for auto-generation.");
    }

    private Schema nullableType(Schema schema) {
        return SchemaBuilder.unionOf()
                .nullType()
                .and()
                .type(schema)
                .endUnion();
    }

    private String convertFieldNameToTypeName(String fieldName) {
        char startCharacter = fieldName.charAt(0);
        if(Character.isAlphabetic(startCharacter)) {
            startCharacter = Character.toUpperCase(startCharacter);
        }

        char[] typeChars = new char[fieldName.length()];
        typeChars[0] = startCharacter;
        fieldName.getChars(1, fieldName.length(), typeChars, 1);
        return new String(typeChars);
    }

}
