package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;


/**
 * This deserializer is used for any Enum classes when converting the pipeline configuration file into the plugin model classes
 * @since 2.11
 */
public class EnumDeserializer extends JsonDeserializer<Enum<?>> implements ContextualDeserializer {

    static final String INVALID_ENUM_VALUE_ERROR_FORMAT = "Invalid value \"%s\". Valid options include %s.";

    private Class<?> enumClass;

    public EnumDeserializer() {}

    public EnumDeserializer(final Class<?> enumClass) {
        if (!enumClass.isEnum()) {
            throw new IllegalArgumentException("The provided class is not an enum: " + enumClass.getName());
        }

        this.enumClass = enumClass;
    }
    @Override
    public Enum<?> deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
        final JsonNode node = p.getCodec().readTree(p);
        final String enumValue = node.asText();

        for (Object enumConstant : enumClass.getEnumConstants()) {
            if (enumConstant.toString().equalsIgnoreCase(enumValue)) {
                return (Enum<?>) enumConstant;
            }
        }

        throw new IllegalArgumentException(String.format(INVALID_ENUM_VALUE_ERROR_FORMAT, enumValue,
                Arrays.stream(enumClass.getEnumConstants()).map(valueEnum -> valueEnum.toString().toLowerCase()).collect(Collectors.toList())));
    }

    @Override
    public JsonDeserializer<?> createContextual(final DeserializationContext ctxt, final BeanProperty property) {
        final JavaType javaType = property.getType();
        final Class<?> rawClass = javaType.getRawClass();

        return new EnumDeserializer(rawClass);
    }
}
