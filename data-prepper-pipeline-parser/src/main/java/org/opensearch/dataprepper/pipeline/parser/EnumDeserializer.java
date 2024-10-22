package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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

        final Optional<Method> jsonCreator = findJsonCreatorMethod();

        try {
            jsonCreator.ifPresent(method -> method.setAccessible(true));

            for (Object enumConstant : enumClass.getEnumConstants()) {
                try {
                    if (jsonCreator.isPresent() && enumConstant.equals(jsonCreator.get().invoke(null, enumValue))) {
                        return (Enum<?>) enumConstant;
                    } else if (jsonCreator.isEmpty() && enumConstant.toString().toLowerCase().equals(enumValue)) {
                        return (Enum<?>) enumConstant;
                    }
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            jsonCreator.ifPresent(method -> method.setAccessible(false));
        }



        final Optional<Method> jsonValueMethod = findJsonValueMethodForClass();
        final List<Object> listOfEnums = jsonValueMethod.map(method -> Arrays.stream(enumClass.getEnumConstants())
                .map(valueEnum -> {
                    try {
                        return method.invoke(valueEnum);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList())).orElseGet(() -> Arrays.stream(enumClass.getEnumConstants())
                .map(valueEnum -> valueEnum.toString().toLowerCase())
                .collect(Collectors.toList()));

        throw new IllegalArgumentException(String.format(INVALID_ENUM_VALUE_ERROR_FORMAT, enumValue, listOfEnums));
    }

    @Override
    public JsonDeserializer<?> createContextual(final DeserializationContext ctxt, final BeanProperty property) {
        final JavaType javaType = property.getType();
        final Class<?> rawClass = javaType.getRawClass();

        return new EnumDeserializer(rawClass);
    }

    private Optional<Method> findJsonValueMethodForClass() {
        for (final Method method : enumClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(JsonValue.class)) {
                return Optional.of(method);
            }
        }

        return Optional.empty();
    }

    private Optional<Method> findJsonCreatorMethod() {
        for (final Method method : enumClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(JsonCreator.class)) {
                return Optional.of(method);
            }
        }

        return Optional.empty();
    }
}
