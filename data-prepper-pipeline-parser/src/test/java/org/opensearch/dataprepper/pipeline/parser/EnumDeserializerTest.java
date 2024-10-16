package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EnumDeserializerTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setup() {
        objectMapper = mock(ObjectMapper.class);
    }

    private EnumDeserializer createObjectUnderTest(final Class<?> enumClass) {
       return new EnumDeserializer(enumClass);
    }

    @Test
    void non_enum_class_throws_IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new EnumDeserializer(Duration.class));
    }

    @ParameterizedTest
    @EnumSource(HandleFailedEventsOption.class)
    void enum_class_returns_expected_enum_constant(final HandleFailedEventsOption handleFailedEventsOption) throws IOException {
        final EnumDeserializer objectUnderTest = createObjectUnderTest(HandleFailedEventsOption.class);
        final JsonParser jsonParser = mock(JsonParser.class);
        final DeserializationContext deserializationContext = mock(DeserializationContext.class);
        when(jsonParser.getCodec()).thenReturn(objectMapper);

        when(objectMapper.readTree(jsonParser)).thenReturn(new TextNode(handleFailedEventsOption.toOptionValue()));

        Enum<?> result = objectUnderTest.deserialize(jsonParser, deserializationContext);

        assertThat(result, equalTo(handleFailedEventsOption));
    }

    @Test
    void enum_class_with_invalid_value_throws_IllegalArgumentException() throws IOException {
        final EnumDeserializer objectUnderTest = createObjectUnderTest(HandleFailedEventsOption.class);
        final JsonParser jsonParser = mock(JsonParser.class);
        final DeserializationContext deserializationContext = mock(DeserializationContext.class);
        when(jsonParser.getCodec()).thenReturn(objectMapper);

        final String invalidValue = UUID.randomUUID().toString();
        when(objectMapper.readTree(jsonParser)).thenReturn(new TextNode(invalidValue));

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                objectUnderTest.deserialize(jsonParser, deserializationContext));

        assertThat(exception, notNullValue());
        final String expectedErrorMessage = "Invalid value \"" + invalidValue + "\". Valid options include";
        assertThat(exception.getMessage(), Matchers.startsWith(expectedErrorMessage));
    }

    @Test
    void create_contextual_returns_expected_enum_deserializer() {
        final DeserializationContext context = mock(DeserializationContext.class);
        final BeanProperty property = mock(BeanProperty.class);

        final ObjectMapper mapper = new ObjectMapper();
        final JavaType javaType = mapper.constructType(HandleFailedEventsOption.class);
        when(property.getType()).thenReturn(javaType);

        final EnumDeserializer objectUnderTest = new EnumDeserializer();
        JsonDeserializer<?> result = objectUnderTest.createContextual(context, property);

        assertTrue(result instanceof EnumDeserializer);
    }
}
