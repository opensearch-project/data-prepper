package org.opensearch.dataprepper.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.util.List;
import java.util.Map;

abstract class AbstractAvroEventConverterTemplate implements AvroEventConverter {
    private final SchemaChooser schemaChooser;

    protected AbstractAvroEventConverterTemplate(final SchemaChooser schemaChooser) {
        this.schemaChooser = schemaChooser;
    }

    @Override
    public GenericRecord convertEventDataToAvro(final Schema schema,
                                                final Map<String, Object> eventData,
                                                final OutputCodecContext codecContext) {
        return convertEventDataToAvro(schema, eventData, codecContext, true);
    }

    private GenericRecord convertEventDataToAvro(final Schema schema,
                                                 final Map<String, Object> eventData,
                                                 final OutputCodecContext codecContext,
                                                 boolean rootOfData) {
        final GenericRecord avroRecord = new GenericData.Record(schema);

        for (String key : getKeyNames(schema, eventData, codecContext, rootOfData)) {
            final Schema.Field field = schema.getField(key);
            if (field == null) {
                throw new RuntimeException("The event has a key ('" + key + "') which is not included in the schema.");
            }
            final Object value = schemaMapper(field, eventData.get(key), codecContext);
            avroRecord.put(key, value);
        }

        return avroRecord;
    }


    private Object schemaMapper(final Schema.Field field, final Object rawValue, OutputCodecContext codecContext) {
        Schema providedSchema = schemaChooser.chooseSchema(field.schema());

        if (providedSchema.getType() == Schema.Type.RECORD && rawValue instanceof Map) {
            return convertEventDataToAvro(providedSchema, (Map<String, Object>) rawValue, codecContext, false);
        } else if (providedSchema.getType() == Schema.Type.ARRAY && rawValue instanceof List) {
            GenericData.Array<Object> avroArray =
                    new GenericData.Array<>(((List<?>) rawValue).size(), providedSchema);
            for (Object element : ((List<?>) rawValue)) {
                avroArray.add(element);
            }
            return avroArray;
        }
        return rawValue;
    }

    /**
     * Template method to get key names for a given object.
     *
     * @param schema The Avro schema
     * @param eventData Current event data
     * @param codecContext The {@link OutputCodecContext}
     * @param rootOfData True, if this is the root of the data. False when this is nested.
     * @return An {@Iterable} of key names.
     */
    abstract Iterable<String> getKeyNames(Schema schema, Map<String, Object> eventData, OutputCodecContext codecContext, boolean rootOfData);
}
