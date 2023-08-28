package org.opensearch.dataprepper.avro;

import org.apache.avro.Schema;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts an Event into an Avro record.
 * <p>
 * This implementation relies on the defined schema. Thus, any fields in the Event which
 * are not in the schema will be ignored.
 */
public class SchemaDefinedAvroEventConverter extends AbstractAvroEventConverterTemplate {
    public SchemaDefinedAvroEventConverter() {
        this(new SchemaChooser());
    }

    SchemaDefinedAvroEventConverter(final SchemaChooser schemaChooser) {
        super(schemaChooser);
    }

    @Override
    Iterable<String> getKeyNames(Schema schema, Map<String, Object> eventData, OutputCodecContext codecContext, boolean rootOfData) {
        return schema.getFields()
                .stream()
                .map(Schema.Field::name)
                .collect(Collectors.toList());
    }
}
