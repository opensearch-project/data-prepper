/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Model class for a Plugin in Configuration YAML containing name of the Plugin and its associated settings
 *
 * @since 1.2
 */
@JsonSerialize(using = PluginModel.PluginModelSerializer.class)
@JsonDeserialize(using = PluginModel.PluginModelDeserializer.class)
public class PluginModel {

    private static final ObjectMapper SERIALIZER_OBJECT_MAPPER;
    
    static {
        SERIALIZER_OBJECT_MAPPER = new ObjectMapper();
        // Note: We don't configure coercion here because our custom deserializer
        // handles all the cases (null, empty, {}, and rejects empty strings)
    }

    private final String pluginName;
    private final InternalJsonModel innerModel;

    /**
     * This class represents the part of the {@link PluginModel} which sits below the name.
     * In the following example, this would be everything below "opensearch":
     * <pre>
     *     opensearch:
     *       hosts: ["http://localhost:9200"]
     *       username: admin
     *       password: admin
     * </pre>
     * <p>
     * Classes that inherit from {@link PluginModel} can create a sublcass of {@link InternalJsonModel}
     * with any custom values needed. By configuring Jackson databind (ie. Jackson annotations) correctly,
     * this internal model will serialize and deserialize correctly.
     */
    static class InternalJsonModel {
        @JsonAnySetter
        @JsonAnyGetter
        private final Map<String, Object> pluginSettings;

        @JsonCreator
        InternalJsonModel() {
            this(new HashMap<>());
        }

        InternalJsonModel(final Map<String, Object> pluginSettings) {
            this.pluginSettings = pluginSettings;
        }
    }

    public PluginModel(final String pluginName, final Map<String, Object> pluginSettings) {
        this(pluginName, new InternalJsonModel(pluginSettings));
    }

    protected PluginModel(final String pluginName, final InternalJsonModel innerModel) {
        this.pluginName = pluginName;
        this.innerModel = Objects.requireNonNull(innerModel);
    }

    public String getPluginName() {
        return pluginName;
    }

    public Map<String, Object> getPluginSettings() {
        return innerModel.pluginSettings;
    }

    <M extends InternalJsonModel> M getInternalJsonModel() {
        return (M) innerModel;
    }

    /**
     * Custom Serializer for Plugin Model
     * <p>
     * Sub-classes of {@link PluginModel} can use this class directly.
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

            // Serialize the inner model to JSON string then back to Map
            // This properly respects all Jackson annotations including @JsonInclude on subclasses
            String jsonString = SERIALIZER_OBJECT_MAPPER.writeValueAsString(value.innerModel);
            Map<String, Object> serializedInner = SERIALIZER_OBJECT_MAPPER.readValue(jsonString, Map.class);

            if (serializedInner.isEmpty()) {
                // Inner model has no content - check if pluginSettings was explicitly null
                // to decide between null and {}
                if (value.innerModel.pluginSettings == null) {
                    // Explicitly null settings -> serialize as null
                    gen.writeObjectField(value.getPluginName(), null);
                } else {
                    // Empty (non-null) settings -> serialize as {}
                    gen.writeFieldName(value.getPluginName());
                    gen.writeStartObject();
                    gen.writeEndObject();
                }
            } else {
                // Inner model has content (plugin settings or subclass fields like routes)
                gen.writeObjectField(value.getPluginName(), serializedInner);
            }
            gen.writeEndObject();
        }
    }

    /**
     * Custom Deserializer for Plugin Model.
     * <p>
     * This deserializer is only intended for {@link PluginModel}. Any
     * subclasses of {@link PluginModel} should see {@link AbstractPluginModelDeserializer}.
     *
     * @since 1.2
     */
    static final class PluginModelDeserializer extends AbstractPluginModelDeserializer<PluginModel, InternalJsonModel> {

        public PluginModelDeserializer() {
            super(PluginModel.class, InternalJsonModel.class, PluginModel::new);
        }
    }

    /**
     * Abstract deserializer for {@link PluginModel} objects. Any classes which inherit from {@link PluginModel}
     * can extend this class. That class must also have a class derived from {@link InternalJsonModel}. It should be configured
     * for Jackson databind. Please note that this class is only intended for internal use. Subclasses of {@link PluginModel}
     * will need to create subclass of {@link AbstractPluginModelDeserializer}, but they do not need to override any methods.
     * The subclass should only need to configure the correct classes in the default constructor.
     *
     * @param <T> The type inheriting from {@link PluginModel} that you ultimately need deserialized
     * @param <M> The type inheriting from {@link InternalJsonModel} that has custom fields.
     *
     * @see SinkModel.SinkModelDeserializer
     */
    abstract static class AbstractPluginModelDeserializer<T extends PluginModel, M extends InternalJsonModel> extends StdDeserializer<PluginModel> {

        private final Class<M> innerModelClass;
        private final BiFunction<String, M, T> constructorFunction;

        protected AbstractPluginModelDeserializer(
                final Class<T> valueClass,
                final Class<M> innerModelClass,
                final BiFunction<String, M, T> constructorFunction) {
            super(valueClass);
            this.innerModelClass = innerModelClass;
            this.constructorFunction = constructorFunction;
        }

        @Override
        public PluginModel deserialize(final JsonParser jsonParser, final DeserializationContext context) throws IOException {
            ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();

            jsonParser.nextToken();

            final String pluginName = jsonParser.currentName();
            jsonParser.nextToken();

            Map<String, Object> data = new HashMap<>();
            if (jsonParser.currentToken() == JsonToken.START_OBJECT) {
                data = mapper.readValue(jsonParser, Map.class);
            } else if (jsonParser.currentToken() == JsonToken.VALUE_NULL) {
                // null value -> treat as empty object (acceptable format)
                data = new HashMap<>();
            } else if (jsonParser.currentToken() == JsonToken.VALUE_STRING) {
                String value = jsonParser.getValueAsString();
                // Empty string "" is NOT allowed - throw exception
                // Any other string value is also not allowed
                if (value.isEmpty()) {
                    throw context.weirdStringException(value, Map.class,
                            "Empty string is not allowed for plugin '" + pluginName + "'. Use null, empty (no value), or {} instead.");
                } else {
                    throw context.weirdStringException(value, Map.class,
                            "String values not allowed for plugin '" + pluginName + "'");
                }
            }
            while (jsonParser.currentToken() != JsonToken.END_OBJECT) {
                jsonParser.nextToken();
            }

            final M innerModel = SERIALIZER_OBJECT_MAPPER.convertValue(data, innerModelClass);

            return constructorFunction.apply(pluginName, innerModel);
        }
    }

}
