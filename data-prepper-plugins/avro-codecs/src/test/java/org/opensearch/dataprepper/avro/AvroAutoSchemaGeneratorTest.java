package org.opensearch.dataprepper.avro;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.File;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AvroAutoSchemaGeneratorTest {
    @Mock
    private OutputCodecContext outputCodecContext;

    private AvroAutoSchemaGenerator createObjectUnderTest() {
        return new AvroAutoSchemaGenerator();
    }

    @ParameterizedTest
    @ArgumentsSource(PrimitiveClassesToTypesArgumentsProvider.class)
    void autoDetermineSchema_with_primitive_type_returns_nullable_of_that_type(Object value, Schema.Type expectedType) {
        String key = randomAvroName();

        Schema schema = createObjectUnderTest().autoDetermineSchema(Map.of(key, value), outputCodecContext);

        assertThat(schema, notNullValue());
        assertThat(schema.getName(), equalTo("Event"));
        assertThat(schema.getFields(), notNullValue());
        assertThat(schema.getFields().size(), equalTo(1));
        Schema.Field field = schema.getField(key);
        assertThat(field, notNullValue());
        assertThat(field.defaultVal(), equalTo(Schema.NULL_VALUE));
        assertThat(field.schema(), notNullValue());
        assertThat(field.schema().isNullable(), equalTo(true));
        assertThat(field.schema().isUnion(), equalTo(true));
        assertThat(field.schema().getTypes(), notNullValue());
        assertThat(field.schema().getTypes().size(), equalTo(2));
        assertThat(field.schema().getTypes().get(0), notNullValue());
        assertThat(field.schema().getTypes().get(0).getType(), equalTo(Schema.Type.NULL));
        assertThat(field.schema().getTypes().get(1), notNullValue());
        assertThat(field.schema().getTypes().get(1).getType(), equalTo(expectedType));
    }

    @ParameterizedTest
    @ArgumentsSource(PrimitiveClassesToTypesArgumentsProvider.class)
    void autoDetermineSchema_with_map_returns_nullable_record_with_nullable_primitives(Object primitiveType, Schema.Type expectedType) {
        String recordKey = randomAvroName();
        String innerKey = randomAvroName();

        Map<String, Object> inputMap = Map.of(recordKey, Map.of(innerKey, primitiveType));
        Schema actualEventSchema = createObjectUnderTest().autoDetermineSchema(inputMap, outputCodecContext);

        assertThat(actualEventSchema, notNullValue());
        assertThat(actualEventSchema.getName(), equalTo("Event"));
        assertThat(actualEventSchema.getFields(), notNullValue());
        assertThat(actualEventSchema.getFields().size(), equalTo(1));
        Schema.Field field = actualEventSchema.getField(recordKey);
        assertThat(field, notNullValue());
        assertThat(field.defaultVal(), equalTo(Schema.NULL_VALUE));
        assertThat(field.schema(), notNullValue());
        assertThat(field.schema().isNullable(), equalTo(true));
        assertThat(field.schema().isUnion(), equalTo(true));
        assertThat(field.schema().getTypes(), notNullValue());
        assertThat(field.schema().getTypes().size(), equalTo(2));
        assertThat(field.schema().getTypes().get(0), notNullValue());
        assertThat(field.schema().getTypes().get(0).getType(), equalTo(Schema.Type.NULL));
        Schema actualRecordSchema = field.schema().getTypes().get(1);
        assertThat(actualRecordSchema, notNullValue());
        assertThat(actualRecordSchema.getType(), equalTo(Schema.Type.RECORD));

        assertThat(actualRecordSchema, notNullValue());
        assertThat(actualRecordSchema.getName(), equalTo(recordKey.replaceFirst("a", "A")));
        assertThat(actualRecordSchema.getFields(), notNullValue());
        assertThat(actualRecordSchema.getFields().size(), equalTo(1));
        Schema.Field actualInnerField = actualRecordSchema.getField(innerKey);
        assertThat(actualInnerField, notNullValue());
        assertThat(actualInnerField.defaultVal(), equalTo(Schema.NULL_VALUE));
        assertThat(actualInnerField.schema(), notNullValue());
        assertThat(actualInnerField.schema().isNullable(), equalTo(true));
        assertThat(actualInnerField.schema().isUnion(), equalTo(true));
        assertThat(actualInnerField.schema().getTypes(), notNullValue());
        assertThat(actualInnerField.schema().getTypes().size(), equalTo(2));
        assertThat(actualInnerField.schema().getTypes().get(0), notNullValue());
        assertThat(actualInnerField.schema().getTypes().get(0).getType(), equalTo(Schema.Type.NULL));
        assertThat(actualInnerField.schema().getTypes().get(1), notNullValue());
        assertThat(actualInnerField.schema().getTypes().get(1).getType(), equalTo(expectedType));
    }

    @ParameterizedTest
    @ArgumentsSource(PrimitiveClassesToTypesArgumentsProvider.class)
    void autoDetermineSchema_with_list_returns_nullable_array_with_nullable_primitives(Object primitiveType, Schema.Type expectedType) {
        String arrayKey = randomAvroName();

        Map<String, Object> inputMap = Map.of(arrayKey, List.of(primitiveType));
        Schema actualEventSchema = createObjectUnderTest().autoDetermineSchema(inputMap, outputCodecContext);

        assertThat(actualEventSchema, notNullValue());
        assertThat(actualEventSchema.getName(), equalTo("Event"));
        assertThat(actualEventSchema.getFields(), notNullValue());
        assertThat(actualEventSchema.getFields().size(), equalTo(1));
        Schema.Field field = actualEventSchema.getField(arrayKey);
        assertThat(field, notNullValue());
        assertThat(field.defaultVal(), equalTo(Schema.NULL_VALUE));
        assertThat(field.schema(), notNullValue());
        assertThat(field.schema().isNullable(), equalTo(true));
        assertThat(field.schema().isUnion(), equalTo(true));
        assertThat(field.schema().getTypes(), notNullValue());
        assertThat(field.schema().getTypes().size(), equalTo(2));
        assertThat(field.schema().getTypes().get(0), notNullValue());
        assertThat(field.schema().getTypes().get(0).getType(), equalTo(Schema.Type.NULL));
        Schema actualArraySchema = field.schema().getTypes().get(1);
        assertThat(actualArraySchema, notNullValue());
        assertThat(actualArraySchema.getType(), equalTo(Schema.Type.ARRAY));

        assertThat(actualArraySchema, notNullValue());
        Schema actualElementType = actualArraySchema.getElementType();
        assertThat(actualElementType, notNullValue());
        assertThat(actualElementType.isNullable(), equalTo(true));
        assertThat(actualElementType.isUnion(), equalTo(true));
        assertThat(actualElementType.getTypes(), notNullValue());
        assertThat(actualElementType.getTypes().size(), equalTo(2));
        assertThat(actualElementType.getTypes().get(0), notNullValue());
        assertThat(actualElementType.getTypes().get(0).getType(), equalTo(Schema.Type.NULL));
        assertThat(actualElementType.getTypes().get(1), notNullValue());
        assertThat(actualElementType.getTypes().get(1).getType(), equalTo(expectedType));
    }

    @Test
    void autoDetermineSchema_with_empty_list_throws() {
        String arrayKey = randomAvroName();

        Map<String, Object> inputMap = Map.of(arrayKey, List.of());
        AvroAutoSchemaGenerator objectUnderTest = createObjectUnderTest();
        SchemaGenerationException actualException = assertThrows(SchemaGenerationException.class, () -> objectUnderTest.autoDetermineSchema(inputMap, outputCodecContext));

        assertThat(actualException.getMessage(), notNullValue());
        assertThat(actualException.getMessage(), containsString(arrayKey));
    }

    @Test
    void autoDetermineSchema_with_null_value_throws() {
        String fieldKey = randomAvroName();

        Map<String, Object> inputMap = Collections.singletonMap(fieldKey, null);
        AvroAutoSchemaGenerator objectUnderTest = createObjectUnderTest();
        SchemaGenerationException actualException = assertThrows(SchemaGenerationException.class, () -> objectUnderTest.autoDetermineSchema(inputMap, outputCodecContext));

        assertThat(actualException.getMessage(), notNullValue());
        assertThat(actualException.getMessage(), containsString(fieldKey));
        assertThat(actualException.getMessage(), containsString("null"));
    }

    @ParameterizedTest
    @ArgumentsSource(SomeUnknownTypesArgumentsProvider.class)
    void autoDetermineSchema_with_unknown_type_throws(Class<?> unknownType) {
        Object value = mock(unknownType);
        String fieldKey = randomAvroName();

        AvroAutoSchemaGenerator objectUnderTest = createObjectUnderTest();
        Map<String, Object> inputMap = Map.of(fieldKey, value);
        SchemaGenerationException actualException = assertThrows(SchemaGenerationException.class, () -> objectUnderTest.autoDetermineSchema(inputMap, outputCodecContext));

        assertThat(actualException.getMessage(), notNullValue());
        assertThat(actualException.getMessage(), containsString(fieldKey));
        assertThat(actualException.getMessage(), containsString(value.getClass().toString()));
    }


    @ParameterizedTest
    @ArgumentsSource(PrimitiveClassesToTypesArgumentsProvider.class)
    void autoDetermineSchema_with_primitive_type_uses_codec_to_include_keys(Object value, Schema.Type expectedType) {
        String includeKey = randomAvroName();
        String notIncludedKey = randomAvroName();

        when(outputCodecContext.shouldNotIncludeKey(includeKey)).thenReturn(false);
        when(outputCodecContext.shouldNotIncludeKey(notIncludedKey)).thenReturn(true);

        Map<String, Object> data = Map.of(
                notIncludedKey, value,
                includeKey, value
        );
        Schema schema = createObjectUnderTest().autoDetermineSchema(data, outputCodecContext);

        assertThat(schema, notNullValue());
        assertThat(schema.getName(), equalTo("Event"));
        assertThat(schema.getFields(), notNullValue());
        assertThat(schema.getFields().size(), equalTo(1));
        assertThat(schema.getField(includeKey), notNullValue());
        assertThat(schema.getField(notIncludedKey), nullValue());
    }


    static class SomeUnknownTypesArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    arguments(Timer.class),
                    arguments(InputStream.class),
                    arguments(File.class)
            );
        }
    }

    private static String randomAvroName() {
        return "a" + UUID.randomUUID().toString().replaceAll("-", "");
    }
}