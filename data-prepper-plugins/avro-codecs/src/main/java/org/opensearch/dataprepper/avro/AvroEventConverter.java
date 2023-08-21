package org.opensearch.dataprepper.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.util.List;
import java.util.Map;

/**
 * Converts an Event into an Avro record.
 * <p>
 * It might be a good idea to consolidate similar logic for input.
 */
public class AvroEventConverter {

    private final SchemaChooser schemaChooser;

    public AvroEventConverter() {
        schemaChooser = new SchemaChooser();
    }

    public GenericRecord convertEventDataToAvro(final Schema schema,
                                                final Map<String, Object> eventData,
                                                OutputCodecContext codecContext) {
        final GenericRecord avroRecord = new GenericData.Record(schema);
        final boolean isExcludeKeyAvailable = !codecContext.getExcludeKeys().isEmpty();
        for (final String key : eventData.keySet()) {
            if (isExcludeKeyAvailable && codecContext.getExcludeKeys().contains(key)) {
                continue;
            }
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
            return convertEventDataToAvro(providedSchema, (Map<String, Object>) rawValue, codecContext);
        } else if (providedSchema.getType() == Schema.Type.ARRAY && rawValue instanceof List) {
            GenericData.Array<String> avroArray =
                    new GenericData.Array<>(((List<String>) rawValue).size(), providedSchema);
            for (String element : ((List<String>) rawValue)) {
                avroArray.add(element);
            }
            return avroArray;
        }
        return rawValue;
    }
}
