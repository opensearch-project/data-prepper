package org.opensearch.dataprepper.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@ExtendWith(MockitoExtension.class)
class SchemaChooserTest {
    private SchemaChooser createObjectUnderTest() {
        return new SchemaChooser();
    }

    @ParameterizedTest
    @ArgumentsSource(PrimitiveTypesArgumentsProvider.class)
    void chooseSchema_should_return_non_nullable_when_nullable_with_primitives(Schema.Type primitiveType) {
        Schema innerSchema = SchemaBuilder.builder().type(primitiveType.getName());
        Schema schema = SchemaBuilder
                .nullable()
                .type(innerSchema);

        Schema actualSchema = createObjectUnderTest().chooseSchema(schema);
        assertThat(actualSchema, notNullValue());
        assertThat(actualSchema.getType(), equalTo(primitiveType));
    }

    @ParameterizedTest
    @ArgumentsSource(PrimitiveTypesArgumentsProvider.class)
    void chooseSchema_should_return_non_nullable_when_nullable_with_record(Schema.Type fieldType) {
        Schema innerSchema = SchemaBuilder.builder().record(randomAvroName())
                .fields()
                .name(randomAvroName()).type(fieldType.getName()).noDefault()
                .endRecord();
        Schema schema = SchemaBuilder
                .nullable()
                .type(innerSchema);

        Schema actualSchema = createObjectUnderTest().chooseSchema(schema);
        assertThat(actualSchema, notNullValue());
        assertThat(actualSchema.getType(), equalTo(Schema.Type.RECORD));
        assertThat(actualSchema.getName(), equalTo(innerSchema.getName()));
    }

    @ParameterizedTest
    @ArgumentsSource(PrimitiveTypesArgumentsProvider.class)
    void chooseSchema_should_return_non_nullable_when_nullable_with_array(Schema.Type itemType) {
        Schema innerSchema = SchemaBuilder.builder()
                .array()
                .items(SchemaBuilder.builder().type(itemType.getName()));

        Schema schema = SchemaBuilder
                .nullable()
                .type(innerSchema);

        Schema actualSchema = createObjectUnderTest().chooseSchema(schema);
        assertThat(actualSchema, notNullValue());
        assertThat(actualSchema.getType(), equalTo(Schema.Type.ARRAY));
        assertThat(actualSchema.getElementType(), notNullValue());
        assertThat(actualSchema.getElementType().getType(), equalTo(itemType));
    }

    @ParameterizedTest
    @ArgumentsSource(PrimitiveTypesArgumentsProvider.class)
    void chooseSchema_with_non_nullable_returns_input_with_non_nullable_primitive_types(Schema.Type primitiveType) {
        Schema inputSchema = SchemaBuilder.builder().type(primitiveType.getName());

        Schema actualSchema = createObjectUnderTest().chooseSchema(inputSchema);
        assertThat(actualSchema, notNullValue());
        assertThat(actualSchema.getType(), equalTo(primitiveType));
    }

    @ParameterizedTest
    @ArgumentsSource(PrimitiveTypesArgumentsProvider.class)
    void chooseSchema_with_non_nullable_returns_input_with_non_nullable_record(Schema.Type fieldType) {
        Schema schema = SchemaBuilder.builder().record(randomAvroName())
                .fields()
                .name(randomAvroName()).type(fieldType.getName()).noDefault()
                .endRecord();

        Schema actualSchema = createObjectUnderTest().chooseSchema(schema);
        assertThat(actualSchema, equalTo(schema));
    }

    @ParameterizedTest
    @ArgumentsSource(PrimitiveTypesArgumentsProvider.class)
    void chooseSchema_with_non_nullable_returns_input_with_non_nullable_array(Schema.Type itemType) {
        Schema schema = SchemaBuilder.builder()
                .array()
                .items(SchemaBuilder.builder().type(itemType.getName()));


        Schema actualSchema = createObjectUnderTest().chooseSchema(schema);
        assertThat(actualSchema, notNullValue());
        assertThat(actualSchema.getType(), equalTo(Schema.Type.ARRAY));
        assertThat(actualSchema.getElementType(), notNullValue());
        assertThat(actualSchema.getElementType().getType(), equalTo(itemType));
    }

    private static String randomAvroName() {
        return "a" + UUID.randomUUID().toString().replaceAll("-", "");
    }

    static class PrimitiveTypesArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    arguments(Schema.Type.STRING),
                    arguments(Schema.Type.INT),
                    arguments(Schema.Type.LONG),
                    arguments(Schema.Type.FLOAT),
                    arguments(Schema.Type.DOUBLE),
                    arguments(Schema.Type.BOOLEAN),
                    arguments(Schema.Type.BYTES)
            );
        }
    }
}