package org.opensearch.dataprepper.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.util.Map;

/**
 * Converts an Event into an Avro record.
 * <p>
 * It might be a good idea to consolidate similar logic for input.
 */
public interface AvroEventConverter {
    /**
     * Converts event data into an Avro record.
     *
     * @param schema The defined Avro schema
     * @param eventData The event data; may include tags
     * @param codecContext The output codec context which may define values included/excluded.
     * @return The generated Avro {@link GenericRecord}.
     */
    GenericRecord convertEventDataToAvro(final Schema schema,
                                         final Map<String, Object> eventData,
                                         final OutputCodecContext codecContext);
}
