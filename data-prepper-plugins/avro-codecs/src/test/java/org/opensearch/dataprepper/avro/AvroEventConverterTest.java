package org.opensearch.dataprepper.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AvroEventConverterTest {
    @Mock
    private Schema schema;

    @Mock
    private OutputCodecContext codecContext;

    @BeforeEach
    void setUp() {
        when(schema.getType()).thenReturn(Schema.Type.RECORD);
    }

    private AvroEventConverter createObjectUnderTest() {
        return new AvroEventConverter();
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

}