package org.opensearch.dataprepper.pipeline.parser.model;

import com.fasterxml.jackson.core.JsonParser;
        import com.fasterxml.jackson.databind.DeserializationContext;
        import com.fasterxml.jackson.databind.ObjectMapper;
        import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

        import java.io.IOException;
        import java.util.Map;

public class TransformerValueDeserializer extends StdDeserializer<Object> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public TransformerValueDeserializer() {
        this(null);
    }

    public TransformerValueDeserializer(final Class<Object> valueClass) {
        super(valueClass);
    }

    @Override
    public Object deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
        final Object value = jsonParser.readValueAs(Object.class);

        try {
            return OBJECT_MAPPER.readValue((String) value, Map.class);
        } catch (final Exception e) {
            return value;
        }
    }
}
