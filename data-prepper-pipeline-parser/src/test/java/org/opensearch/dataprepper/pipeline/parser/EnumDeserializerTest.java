package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
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
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    @EnumSource(TestEnum.class)
    void enum_class_with_json_creator_annotation_returns_expected_enum_constant(final TestEnum testEnumOption) throws IOException {
        final EnumDeserializer objectUnderTest = createObjectUnderTest(TestEnum.class);
        final JsonParser jsonParser = mock(JsonParser.class);
        final DeserializationContext deserializationContext = mock(DeserializationContext.class);
        when(jsonParser.getCodec()).thenReturn(objectMapper);

        when(objectMapper.readTree(jsonParser)).thenReturn(new TextNode(testEnumOption.toString()));

        Enum<?> result = objectUnderTest.deserialize(jsonParser, deserializationContext);

        assertThat(result, equalTo(testEnumOption));
    }

    @ParameterizedTest
    @EnumSource(TestEnumWithJsonValue.class)
    void enum_class_with_no_json_creator_and_a_json_value_annotation_returns_expected_enum_constant(final TestEnumWithJsonValue testEnumOption) throws IOException {
        final EnumDeserializer objectUnderTest = createObjectUnderTest(TestEnumWithJsonValue.class);
        final JsonParser jsonParser = mock(JsonParser.class);
        final DeserializationContext deserializationContext = mock(DeserializationContext.class);
        when(jsonParser.getCodec()).thenReturn(objectMapper);

        when(objectMapper.readTree(jsonParser)).thenReturn(new TextNode(testEnumOption.toString()));

        Enum<?> result = objectUnderTest.deserialize(jsonParser, deserializationContext);

        assertThat(result, equalTo(testEnumOption));
    }

    @ParameterizedTest
    @EnumSource(TestEnumOnlyUppercase.class)
    void enum_class_with_just_enum_values_returns_expected_enum_constant(final TestEnumOnlyUppercase testEnumOption) throws IOException {
        final EnumDeserializer objectUnderTest = createObjectUnderTest(TestEnumOnlyUppercase.class);
        final JsonParser jsonParser = mock(JsonParser.class);
        final DeserializationContext deserializationContext = mock(DeserializationContext.class);
        when(jsonParser.getCodec()).thenReturn(objectMapper);

        when(objectMapper.readTree(jsonParser)).thenReturn(new TextNode(testEnumOption.name()));

        Enum<?> result = objectUnderTest.deserialize(jsonParser, deserializationContext);

        assertThat(result, equalTo(testEnumOption));
    }

    @ParameterizedTest
    @EnumSource(TestEnumWithoutJsonCreator.class)
    void enum_class_without_json_creator_or_json_value_annotation_returns_expected_enum_constant(final TestEnumWithoutJsonCreator enumWithoutJsonCreator) throws IOException {
        final EnumDeserializer objectUnderTest = createObjectUnderTest(TestEnumWithoutJsonCreator.class);
        final JsonParser jsonParser = mock(JsonParser.class);
        final DeserializationContext deserializationContext = mock(DeserializationContext.class);
        when(jsonParser.getCodec()).thenReturn(objectMapper);

        when(objectMapper.readTree(jsonParser)).thenReturn(new TextNode(enumWithoutJsonCreator.name()));

        Enum<?> result = objectUnderTest.deserialize(jsonParser, deserializationContext);

        assertThat(result, equalTo(enumWithoutJsonCreator));
    }

    @Test
    void enum_class_with_invalid_value_and_jsonValue_annotation_throws_IllegalArgumentException() throws IOException {
        final EnumDeserializer objectUnderTest = createObjectUnderTest(TestEnum.class);
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
        assertThat(exception.getMessage(), containsString("[test_display_one, test_display_two, test_display_three]"));
    }

    @Test
    void enum_class_with_invalid_value_and_no_jsonValue_annotation_throws_IllegalArgumentException() throws IOException {
        final EnumDeserializer objectUnderTest = createObjectUnderTest(TestEnumWithoutJsonCreator.class);
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
        assertThat(exception.getMessage(), containsString("[TEST]"));
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

        assertThat(result, instanceOf(EnumDeserializer.class));
    }

    private enum TestEnum {
        TEST_ONE("test_display_one"),
        TEST_TWO("test_display_two"),
        TEST_THREE("test_display_three");
        private static final Map<String, TestEnum> NAMES_MAP = Arrays.stream(TestEnum.values())
                .collect(Collectors.toMap(TestEnum::toString, Function.identity()));
        private final String name;
        TestEnum(final String name) {
            this.name = name;
        }

        @JsonValue
        public String toString() {
            return this.name;
        }
        @JsonCreator
        static TestEnum fromOptionValue(final String option) {
            return NAMES_MAP.get(option);
        }
    }

    private enum TestEnumWithJsonValue {
        TEST_ONE("test_json_value_one"),
        TEST_TWO("test_json_value_two"),
        TEST_THREE("test_json_value_three");
        private static final Map<String, TestEnum> NAMES_MAP = Arrays.stream(TestEnum.values())
                .collect(Collectors.toMap(TestEnum::toString, Function.identity()));
        private final String name;
        TestEnumWithJsonValue(final String name) {
            this.name = name;
        }

        @JsonValue
        public String toString() {
            return this.name;
        }

        static TestEnum fromOptionValue(final String option) {
            return NAMES_MAP.get(option);
        }
    }

    private enum TestEnumWithoutJsonCreator {
        TEST("test");
        private static final Map<String, TestEnumWithoutJsonCreator> NAMES_MAP = Arrays.stream(TestEnumWithoutJsonCreator.values())
                .collect(Collectors.toMap(TestEnumWithoutJsonCreator::toString, Function.identity()));
        private final String name;
        TestEnumWithoutJsonCreator(final String name) {
            this.name = name;
        }
        public String toString() {
            return UUID.randomUUID().toString();
        }

        static TestEnumWithoutJsonCreator fromOptionValue(final String option) {
            return NAMES_MAP.get(option);
        }
    }

    private enum TestEnumOnlyUppercase {
        VALUE_ONE,
        VALUE_TWO;
    }
}
