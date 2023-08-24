package org.opensearch.dataprepper.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

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
        this(new SchemaChooser());
    }

    AvroEventConverter(final SchemaChooser schemaChooser) {
        this.schemaChooser = schemaChooser;
    }

    public GenericRecord convertEventDataToAvro(final Schema schema,
                                                final Map<String, Object> eventData) {
        final GenericRecord avroRecord = new GenericData.Record(schema);

        for (Schema.Field field : schema.getFields()) {
            String key = field.name();
            final Object value = schemaMapper(field, eventData.get(key));
            avroRecord.put(key, value);
        }

        return avroRecord;
    }

    private Object schemaMapper(final Schema.Field field, final Object rawValue) {
        Schema providedSchema = schemaChooser.chooseSchema(field.schema());

        if (providedSchema.getType() == Schema.Type.RECORD && rawValue instanceof Map) {
            return convertEventDataToAvro(providedSchema, (Map<String, Object>) rawValue);
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
}
