package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.HashMap;

/**
 * Custom Deserializer for Plugin Model
 *
 * @since 1.2
 */
public class PluginModelDeserializer extends StdDeserializer<PluginModel> {

    public PluginModelDeserializer() {
        this(null);
    }

    public PluginModelDeserializer(Class<PluginModel> t) {
        super(t);
    }

    @Override
    public PluginModel deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JacksonException {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);
        String pluginName = node.get("pluginName").asText();
        return new PluginModel(pluginName, new HashMap<>());
    }
}
