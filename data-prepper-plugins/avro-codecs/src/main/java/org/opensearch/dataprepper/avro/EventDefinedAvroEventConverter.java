package org.opensearch.dataprepper.avro;

import org.apache.avro.Schema;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Converts an Event into an Avro record.
 * <p>
 * This implementation utilizes the Event data first to populate the Avro record. Thus,
 * it will fail if the Event has any fields not in the schema.
 */
public class EventDefinedAvroEventConverter extends AbstractAvroEventConverterTemplate {
    public EventDefinedAvroEventConverter() {
        this(new SchemaChooser());
    }

    EventDefinedAvroEventConverter(final SchemaChooser schemaChooser) {
        super(schemaChooser);
    }

    @Override
    Iterable<String> getKeyNames(Schema schema, Map<String, Object> eventData, OutputCodecContext codecContext, boolean rootOfData) {
        Set<String> keySet = eventData.keySet();

        if(codecContext == null || !rootOfData) {
            return keySet;
        }
        else {
            return keySet.stream()
                    .filter(Predicate.not(codecContext::shouldNotIncludeKey))
                    .collect(Collectors.toList());
        }
    }
}
