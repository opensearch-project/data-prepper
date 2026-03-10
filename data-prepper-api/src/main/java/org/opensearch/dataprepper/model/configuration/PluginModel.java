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
import com.fasterxml.jackson.databind.JsonMappingException;
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
import java.util.function.Supplier;

/**
 * Model class for a Plugin in Configuration YAML containing name of the Plugin and its associated settings
 *
 * @since 1.2
 */
@JsonSerialize(using = PluginModel.PluginModelSerializer.class)
@JsonDeserialize(using = PluginModel.PluginModelDeserializer.class)
public class PluginModel {

    private static final ObjectMapper SERIALIZER_OBJECT_MAPPER = new ObjectMapper();

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
        private final Map<String, Object> pluginSettings;

        @JsonCreator
        InternalJsonModel() {
            this(new HashMap<>());
        }

        InternalJsonModel(final Map<String, Object> pluginSettings) {
            this.pluginSettings = pluginSettings;
        }

        @JsonAnyGetter
        Map<String, Object> getPluginSettingsForSerialization() {
            return pluginSettings != null ? pluginSettings : new HashMap<>();
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

            // Serialize innerModel to a Map via a JSON round-trip so that all Jackson-annotated
            // fields on potential subclasses of InternalJsonModel (e.g. SinkInternalJsonModel with
            // routes, tagsTargetKey, etc.) are included. Directly reading pluginSettings would miss
            // those extra fields. The resulting Map is then inspected to distinguish between a
            // truly empty/null inner model and one that has actual content to write.
            final String jsonString = SERIALIZER_OBJECT_MAPPER.writeValueAsString(value.innerModel);
            final Map<String, Object> serializedInner = SERIALIZER_OBJECT_MAPPER.readValue(jsonString, Map.class);

            if (serializedInner.isEmpty()) {
                // Empty inner model: output null if pluginSettings was null, {} if it was an empty map
                if (value.innerModel.pluginSettings == null) {
                    gen.writeObjectField(value.getPluginName(), null);
                } else {
                    gen.writeFieldName(value.getPluginName());
                    gen.writeStartObject();
                    gen.writeEndObject();
                }
            } else {
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
            super(PluginModel.class, InternalJsonModel.class, PluginModel::new, () -> new InternalJsonModel(null));
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
        private final Supplier<M> nullSettingsModelSupplier;

        protected AbstractPluginModelDeserializer(
                final Class<T> valueClass,
                final Class<M> innerModelClass,
                final BiFunction<String, M, T> constructorFunction,
                final Supplier<M> nullSettingsModelSupplier) {
            super(valueClass);
            this.innerModelClass = innerModelClass;
            this.constructorFunction = constructorFunction;
            this.nullSettingsModelSupplier = nullSettingsModelSupplier;
        }

        @Override
        public PluginModel deserialize(final JsonParser jsonParser, final DeserializationContext context) throws IOException {
            final ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();

            jsonParser.nextToken();

            final String pluginName = jsonParser.currentName();
            jsonParser.nextToken();

            boolean isNull = false;
            Map<String, Object> data = null;
            if (jsonParser.currentToken() == JsonToken.START_OBJECT) {
                data = mapper.readValue(jsonParser, Map.class);
                // readValue consumed up to the inner END_OBJECT; advance to the outer END_OBJECT
                jsonParser.nextToken();
            } else if (jsonParser.currentToken() == JsonToken.VALUE_NULL) {
                // null or no-value (stdout: null / stdout:) -> preserve as null settings
                isNull = true;
                // advance to the outer END_OBJECT
                jsonParser.nextToken();
            } else if (jsonParser.currentToken() == JsonToken.VALUE_STRING) {
                final String value = jsonParser.getValueAsString();
                if (value.isEmpty()) {
                    throw context.weirdStringException(value, Map.class,
                            "Empty string is not allowed for plugin '" + pluginName + "'. Use null, empty (no value), or {} instead.");
                } else {
                    throw context.weirdStringException(value, Map.class,
                            "String values not allowed for plugin '" + pluginName + "'");
                }
            } else {
                throw JsonMappingException.from(jsonParser,
                        "Unexpected value for plugin '" + pluginName + "': expected an object, null, or no value, but got " +
                        jsonParser.currentToken());
            }

            final M innerModel = isNull
                    ? nullSettingsModelSupplier.get()
                    : SERIALIZER_OBJECT_MAPPER.convertValue(data, innerModelClass);
            return constructorFunction.apply(pluginName, innerModel);
        }
    }

}
