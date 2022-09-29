/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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
            Map<String, Object> serializedInner = SERIALIZER_OBJECT_MAPPER.convertValue(value.innerModel, Map.class);
            if(serializedInner != null && serializedInner.isEmpty())
                serializedInner = null;
            gen.writeObjectField(value.getPluginName(), serializedInner);
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
            super(PluginModel.class, InternalJsonModel.class, PluginModel::new, InternalJsonModel::new);
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
        private final Supplier<M> emptyInnerModelConstructor;

        protected AbstractPluginModelDeserializer(
                final Class<T> valueClass,
                final Class<M> innerModelClass,
                final BiFunction<String, M, T> constructorFunction,
                final Supplier<M> emptyInnerModelConstructor) {
            super(valueClass);
            this.innerModelClass = innerModelClass;
            this.constructorFunction = constructorFunction;
            this.emptyInnerModelConstructor = emptyInnerModelConstructor;
        }

        @Override
        public PluginModel deserialize(final JsonParser jsonParser, final DeserializationContext context) throws IOException, JacksonException {
            final JsonNode node = jsonParser.getCodec().readTree(jsonParser);

            final Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            final Map.Entry<String, JsonNode> onlyField = fields.next();

            final String pluginName = onlyField.getKey();
            final JsonNode value = onlyField.getValue();

            M innerModel = SERIALIZER_OBJECT_MAPPER.convertValue(value, innerModelClass);
            if(innerModel == null)
                innerModel = emptyInnerModelConstructor.get();

            return constructorFunction.apply(pluginName, innerModel);
        }
    }

}
