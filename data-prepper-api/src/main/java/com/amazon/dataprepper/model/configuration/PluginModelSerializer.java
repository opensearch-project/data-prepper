package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Custom Serializer for Plugin Model
 *
 * @since 1.2
 */
public class PluginModelSerializer extends StdSerializer<PluginModel> {

    private final ObjectMapper mapper = new ObjectMapper();

    public PluginModelSerializer() {
        this(null);
    }

    public PluginModelSerializer(Class<PluginModel> t) {
        super(t);
    }

    @Override
    public void serialize(
            PluginModel value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        StringWriter writer = new StringWriter();
        mapper.writeValue(writer, value.getPluginSettings());
        gen.writeStartObject();
        gen.writeStringField("pluginName", value.getPluginName());
        gen.writeFieldName(writer.toString());
        gen.writeEndObject();
    }
}
