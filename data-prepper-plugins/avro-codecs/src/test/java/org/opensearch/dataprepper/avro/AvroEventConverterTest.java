package org.opensearch.dataprepper.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AvroEventConverterTest {
    @Mock
    private SchemaChooser schemaChooser;
    @Mock(lenient = true)
    private Schema schema;

    @Mock
    private OutputCodecContext codecContext;

    @BeforeEach
    void setUp() {
        when(schema.getType()).thenReturn(Schema.Type.RECORD);
    }


    private AvroEventConverter createObjectUnderTest() {
        return new AvroEventConverter(schemaChooser);
    }

    @ParameterizedTest
    @ArgumentsSource(PrimitiveClassesToTypesArgumentsProvider.class)
    void convertEventDataToAvro_does_not_need_to_getField_on_empty_map() {
        Map<String, Object> data = Collections.emptyMap();
        GenericRecord actualRecord = createObjectUnderTest().convertEventDataToAvro(schema, data, codecContext);

        assertThat(actualRecord, notNullValue());
        assertThat(actualRecord.getSchema(), equalTo(schema));

        verify(schema, never()).getField(anyString());
    }

    @Nested
    class WithField {

        private String fieldName;
        @Mock(lenient = true)
        private Schema.Field field;
        @Mock
        private Schema fieldSchema;

        @BeforeEach
        void setUp() {
            fieldName = UUID.randomUUID().toString();
            when(schema.getField(fieldName)).thenReturn(field);
            when(schema.getFields()).thenReturn(Collections.singletonList(field));
            when(field.schema()).thenReturn(fieldSchema);
            when(field.pos()).thenReturn(0);
        }

        @ParameterizedTest
        @ArgumentsSource(PrimitiveClassesToTypesArgumentsProvider.class)
        void convertEventDataToAvro_adds_fields(Object value, Schema.Type expectedType) {
            Map<String, Object> data = Map.of(fieldName, value);
            when(fieldSchema.getType()).thenReturn(expectedType);

            when(schemaChooser.chooseSchema(fieldSchema)).thenReturn(fieldSchema);

            GenericRecord actualRecord = createObjectUnderTest().convertEventDataToAvro(schema, data, codecContext);

            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getSchema(), equalTo(schema));

            assertThat(actualRecord.get(fieldName), notNullValue());
            assertThat(actualRecord.get(fieldName), instanceOf(value.getClass()));
            assertThat(actualRecord.get(fieldName), equalTo(value));
        }

        @ParameterizedTest
        @ArgumentsSource(PrimitiveClassesToTypesArgumentsProvider.class)
        void convertEventDataToAvro_skips_files_if_should_not_include(Object value, Schema.Type expectedType) {
            Map<String, Object> data = Map.of(fieldName, value);
            when(codecContext.shouldNotIncludeKey(fieldName)).thenReturn(true);

            GenericRecord actualRecord = createObjectUnderTest().convertEventDataToAvro(schema, data, codecContext);

            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getSchema(), equalTo(schema));

            assertThat(actualRecord.get(fieldName), nullValue());
        }
    }
}