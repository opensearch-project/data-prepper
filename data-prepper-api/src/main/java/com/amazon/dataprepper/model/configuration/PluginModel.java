package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Model class for a Plugin in Configuration YAML containing name of the Plugin and its associated settings
 *
 * @since 1.2
 */
@JsonSerialize(using = PluginModel.PluginModelSerializer.class)
@JsonDeserialize(using = PluginModel.PluginModelDeserializer.class)
public class PluginModel {

    private final String pluginName;
    private final Map<String, Object> pluginSettings;

    public PluginModel(final String pluginName, final Map<String, Object> pluginSettings) {
        this.pluginName = pluginName;
        this.pluginSettings = pluginSettings;
    }

    public String getPluginName() {
        return pluginName;
    }

    public Map<String, Object> getPluginSettings() {
        return pluginSettings;
    }

    /**
     * Custom Serializer for Plugin Model
     *
     * @since 1.2
     */
    static class PluginModelSerializer extends StdSerializer<PluginModel> {

        public PluginModelSerializer() {
            this(null);
        }

        public PluginModelSerializer(final Class<PluginModel> valueClass) {
            super(valueClass);
        }

        @Override
        public void serialize(
                final PluginModel value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
            gen.writeStartObject();
            gen.writeObjectField(value.getPluginName(), value.getPluginSettings());
            gen.writeEndObject();
        }
    }

    /**
     * Custom Deserializer for Plugin Model
     *
     * @since 1.2
     */
    static class PluginModelDeserializer extends StdDeserializer<PluginModel> {

        static final ObjectMapper mapper = new ObjectMapper();

        public PluginModelDeserializer() {
            this(null);
        }

        public PluginModelDeserializer(final Class<PluginModel> valueClass) {
            super(valueClass);
        }

        @Override
        public PluginModel deserialize(final JsonParser jsonParser, final DeserializationContext context) throws IOException, JacksonException {
            final JsonNode node = jsonParser.getCodec().readTree(jsonParser);

            final Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            Map.Entry<String, JsonNode> onlyField = fields.next();

            final String pluginName = onlyField.getKey();
            final JsonNode value = onlyField.getValue();

            final Map<String, Object> settingsMap = mapper.convertValue(value, new TypeReference<Map<String, Object>>(){});
            return new PluginModel(pluginName, settingsMap);
        }
    }

}
