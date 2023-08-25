/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class AvroOutputCodecTest {
    private static final String EXPECTED_SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"Event\",\"fields\":" +
            "[{\"name\":\"myDouble\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"myLong\",\"type\":[\"null\",\"long\"],\"default\":null}," +
            "{\"name\":\"myArray\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"string\"]}],\"default\":null}," +
            "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}," +
            "{\"name\":\"nestedRecord\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"NestedRecord\",\"fields\":[{\"name\":\"secondFieldInNestedRecord\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"firstFieldInNestedRecord\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null}," +
            "{\"name\":\"age\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"myFloat\",\"type\":[\"null\",\"float\"],\"default\":null}]}";
    public static final int TOTAL_TOP_LEVEL_FIELDS = 7;
    private AvroOutputCodecConfig config;

    private ByteArrayOutputStream outputStream;

    @BeforeEach
    void setUp() {
        config = new AvroOutputCodecConfig();
        config.setSchema(createStandardSchema().toString());
    }

    private AvroOutputCodec createObjectUnderTest() {
        return new AvroOutputCodec(config);
    }

    @Test
    void constructor_throws_if_schema_is_invalid() {
        String invalidSchema = createStandardSchema().toString().replaceAll(",", ";");
        config.setSchema(invalidSchema);

        RuntimeException actualException = assertThrows(RuntimeException.class, this::createObjectUnderTest);

        assertThat(actualException.getMessage(), notNullValue());
        assertThat(actualException.getMessage(), containsString(invalidSchema));
        assertThat(actualException.getMessage(), containsString("was expecting comma"));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_happy_case(final int numberOfRecords) throws Exception {
        AvroOutputCodec avroOutputCodec = createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        OutputCodecContext codecContext = new OutputCodecContext();
        avroOutputCodec.start(outputStream, null, codecContext);
        List<Map<String, Object>> inputMaps = generateRecords(numberOfRecords);
        for (Map<String, Object> inputMap : inputMaps) {
            final Event event = createEventRecord(inputMap);
            avroOutputCodec.writeEvent(event, outputStream);
        }
        avroOutputCodec.complete(outputStream);

        final List<GenericRecord> actualAvroRecords = createAvroRecordsList(outputStream);
        assertThat(actualAvroRecords.size(), equalTo(numberOfRecords));

        int index = 0;
        for (final GenericRecord actualRecord : actualAvroRecords) {

            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getSchema(), notNullValue());

            final Map<String, Object> expectedMap = inputMaps.get(index);
            final Map<String, Object> actualMap = new HashMap<>();

            for (Schema.Field field : actualRecord.getSchema().getFields()) {
                if (actualRecord.get(field.name()) instanceof GenericRecord) {
                    GenericRecord nestedRecord = (GenericRecord) actualRecord.get(field.name());
                    actualMap.put(field.name(), convertRecordToMap(nestedRecord));
                } else if(actualRecord.get(field.name()) instanceof GenericArray) {
                    GenericArray genericArray = (GenericArray) actualRecord.get(field.name());
                    actualMap.put(field.name(), genericArray.stream().map(AvroOutputCodecTest::decodeOutputIfEncoded).collect(Collectors.toList()));
                }
                else {
                    Object decodedActualOutput = decodeOutputIfEncoded(actualRecord.get(field.name()));
                    actualMap.put(field.name(), decodedActualOutput);
                }
            }
            assertThat(actualMap, equalTo(expectedMap));
            index++;
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_happy_case_nullable_records(final int numberOfRecords) throws Exception {
        config.setSchema(createStandardSchemaNullable().toString());
        AvroOutputCodec avroOutputCodec = createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        OutputCodecContext codecContext = new OutputCodecContext();
        avroOutputCodec.start(outputStream, null, codecContext);
        List<Map<String, Object>> inputMaps = generateRecords(numberOfRecords);
        for (Map<String, Object> inputMap : inputMaps) {
            final Event event = createEventRecord(inputMap);
            avroOutputCodec.writeEvent(event, outputStream);
        }
        avroOutputCodec.complete(outputStream);

        final List<GenericRecord> actualAvroRecords = createAvroRecordsList(outputStream);
        assertThat(actualAvroRecords.size(), equalTo(numberOfRecords));


        int index = 0;
        for (final GenericRecord actualRecord : actualAvroRecords) {

            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getSchema(), notNullValue());

            final Map<String, Object> expectedMap = inputMaps.get(index);
            final Map<String, Object> actualMap = new HashMap<>();

            for (Schema.Field field : actualRecord.getSchema().getFields()) {
                if (field.name().equals("nestedRecord")) {
                    GenericRecord nestedRecord = (GenericRecord) actualRecord.get(field.name());
                    actualMap.put(field.name(), convertRecordToMap(nestedRecord));
                } else if(field.name().equals("myArray")) {
                    GenericArray<?> genericArray = (GenericArray<?>) actualRecord.get(field.name());
                    List<?> items = genericArray.stream()
                            .map(AvroOutputCodecTest::decodeOutputIfEncoded)
                            .collect(Collectors.toList());
                    actualMap.put(field.name(), items);
                } else {
                    Object decodedActualOutput = decodeOutputIfEncoded(actualRecord.get(field.name()));
                    actualMap.put(field.name(), decodedActualOutput);
                }
            }
            assertThat(actualMap, equalTo(expectedMap));
            index++;
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_happy_case_nullable_records_with_empty_maps(final int numberOfRecords) throws Exception {
        config.setSchema(createStandardSchemaNullable().toString());
        AvroOutputCodec objectUnderTest = createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        OutputCodecContext codecContext = new OutputCodecContext();
        objectUnderTest.start(outputStream, null, codecContext);
        List<Map<String, Object>> inputMaps = generateEmptyRecords(numberOfRecords);
        for (Map<String, Object> inputMap : inputMaps) {
            final Event event = createEventRecord(inputMap);
            objectUnderTest.writeEvent(event, outputStream);
        }
        objectUnderTest.complete(outputStream);

        final List<GenericRecord> actualAvroRecords = createAvroRecordsList(outputStream);
        assertThat(actualAvroRecords.size(), equalTo(numberOfRecords));


        int count = 0;
        for (final GenericRecord actualRecord : actualAvroRecords) {

            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getSchema(), notNullValue());

            List<Schema.Field> fields = actualRecord.getSchema().getFields();
            assertThat(fields.size(), equalTo(TOTAL_TOP_LEVEL_FIELDS));
            for (Schema.Field field : fields) {
                Object actualValue = actualRecord.get(field.name());
                assertThat(actualValue, nullValue());
            }
            count++;
        }

        assertThat(count, equalTo(inputMaps.size()));
    }

    @Test
    void writeEvent_accepts_event_when_field_does_not_exist_in_user_defined_schema() throws IOException {
        final String invalidFieldName = UUID.randomUUID().toString();

        Map<String, Object> mapWithInvalid = generateRecords(1).get(0);
        mapWithInvalid.put(invalidFieldName, UUID.randomUUID().toString());
        final Event eventWithInvalidField = mock(Event.class);
        when(eventWithInvalidField.toMap()).thenReturn(mapWithInvalid);

        final AvroOutputCodec objectUnderTest = createObjectUnderTest();

        outputStream = new ByteArrayOutputStream();
        objectUnderTest.start(outputStream, null, new OutputCodecContext());

        objectUnderTest.writeEvent(eventWithInvalidField, outputStream);
        objectUnderTest.complete(outputStream);

        final List<GenericRecord> actualAvroRecords = createAvroRecordsList(outputStream);
        assertThat(actualAvroRecords.size(), equalTo(1));

        int count = 0;
        for (final GenericRecord actualRecord : actualAvroRecords) {

            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getSchema(), notNullValue());

            List<Schema.Field> fields = actualRecord.getSchema().getFields();
            assertThat(fields.size(), equalTo(TOTAL_TOP_LEVEL_FIELDS));
            for (Schema.Field field : fields) {
                Object actualValue = actualRecord.get(field.name());
                assertThat(actualValue, notNullValue());
            }
            count++;
        }

        assertThat(count, equalTo(1));
    }

    @Test
    void writeEvent_throws_exception_when_field_does_not_exist_in_auto_schema() throws IOException {
        config.setSchema(null);
        final String invalidFieldName = UUID.randomUUID().toString();

        Map<String, Object> mapWithInvalid = generateRecords(1).get(0);
        mapWithInvalid.put(invalidFieldName, UUID.randomUUID().toString());
        final Event eventWithInvalidField = mock(Event.class);
        when(eventWithInvalidField.toMap()).thenReturn(mapWithInvalid);

        final AvroOutputCodec objectUnderTest = createObjectUnderTest();

        outputStream = new ByteArrayOutputStream();
        objectUnderTest.start(outputStream, createEventRecord(generateRecords(1).get(0)), new OutputCodecContext());

        final RuntimeException actualException = assertThrows(RuntimeException.class, () -> objectUnderTest.writeEvent(eventWithInvalidField, outputStream));

        assertThat(actualException.getMessage(), notNullValue());
        assertThat(actualException.getMessage(), containsString(invalidFieldName));
    }


    @Test
    public void testInlineSchemaBuilder() throws IOException {
        Schema expectedSchema = new Schema.Parser().parse(EXPECTED_SCHEMA_STRING);
        AvroOutputCodec avroOutputCodec = createObjectUnderTest();
        Event event = createEventRecord(generateRecords(1).get(0));
        Schema actualSchema = avroOutputCodec.buildInlineSchemaFromEvent(event);
        assertThat(actualSchema, equalTo(expectedSchema));
    }

    @Nested
    class ValidateWithSchema {
        private OutputCodecContext codecContext;
        private List<String> keys;

        @BeforeEach
        void setUp() {
            config.setSchema(createStandardSchemaNullable().toString());
            codecContext = mock(OutputCodecContext.class);
            keys = List.of(UUID.randomUUID().toString());
        }

        @Test
        void validateAgainstCodecContext_throws_when_user_defined_schema_and_includeKeys_non_empty() {
            when(codecContext.getIncludeKeys()).thenReturn(keys);

            AvroOutputCodec objectUnderTest = createObjectUnderTest();
            assertThrows(InvalidPluginConfigurationException.class, () -> objectUnderTest.validateAgainstCodecContext(codecContext));
        }

        @Test
        void validateAgainstCodecContext_throws_when_user_defined_schema_and_excludeKeys_non_empty() {
            when(codecContext.getExcludeKeys()).thenReturn(keys);

            AvroOutputCodec objectUnderTest = createObjectUnderTest();
            assertThrows(InvalidPluginConfigurationException.class, () -> objectUnderTest.validateAgainstCodecContext(codecContext));
        }

        @Test
        void validateAgainstCodecContext_is_ok_when_user_defined_schema_and_includeKeys_isNull() {
            when(codecContext.getIncludeKeys()).thenReturn(null);

            createObjectUnderTest().validateAgainstCodecContext(codecContext);
        }

        @Test
        void validateAgainstCodecContext_is_ok_when_user_defined_schema_and_includeKeys_isEmpty() {
            when(codecContext.getIncludeKeys()).thenReturn(Collections.emptyList());

            createObjectUnderTest().validateAgainstCodecContext(codecContext);
        }

        @Test
        void validateAgainstCodecContext_is_ok_when_user_defined_schema_and_excludeKeys_isNull() {
            when(codecContext.getExcludeKeys()).thenReturn(null);

            createObjectUnderTest().validateAgainstCodecContext(codecContext);
        }

        @Test
        void validateAgainstCodecContext_is_ok_when_user_defined_schema_and_excludeKeys_isEmpty() {
            when(codecContext.getExcludeKeys()).thenReturn(Collections.emptyList());

            createObjectUnderTest().validateAgainstCodecContext(codecContext);
        }
    }

    @Nested
    class ValidateWithAutoSchema {
        private OutputCodecContext codecContext;
        private List<String> keys;

        @BeforeEach
        void setUp() {
            config.setAutoSchema(true);
            codecContext = mock(OutputCodecContext.class, withSettings().lenient());
            keys = List.of(UUID.randomUUID().toString());
        }

        @Test
        void validateAgainstCodecContext_is_ok_when_auto_schema_and_includeKeys_non_empty() {
            when(codecContext.getIncludeKeys()).thenReturn(keys);

            createObjectUnderTest().validateAgainstCodecContext(codecContext);
        }

        @Test
        void validateAgainstCodecContext_is_ok_when_auto_schema_and_excludeKeys_non_empty() {
            when(codecContext.getExcludeKeys()).thenReturn(keys);

            createObjectUnderTest().validateAgainstCodecContext(codecContext);
        }

        @Test
        void validateAgainstCodecContext_is_ok_when_auto_schema_and_includeKeys_isNull() {
            when(codecContext.getIncludeKeys()).thenReturn(null);

            createObjectUnderTest().validateAgainstCodecContext(codecContext);
        }

        @Test
        void validateAgainstCodecContext_is_ok_when_auto_schema_and_includeKeys_isEmpty() {
            when(codecContext.getIncludeKeys()).thenReturn(Collections.emptyList());

            createObjectUnderTest().validateAgainstCodecContext(codecContext);
        }

        @Test
        void validateAgainstCodecContext_is_ok_when_auto_schema_and_excludeKeys_isNull() {
            when(codecContext.getExcludeKeys()).thenReturn(null);

            createObjectUnderTest().validateAgainstCodecContext(codecContext);
        }

        @Test
        void validateAgainstCodecContext_is_ok_when_auto_schema_and_excludeKeys_isEmpty() {
            when(codecContext.getExcludeKeys()).thenReturn(Collections.emptyList());

            createObjectUnderTest().validateAgainstCodecContext(codecContext);
        }
    }

    private static Event createEventRecord(final Map<String, Object> eventData) {
        return JacksonLog.builder().withData(eventData).build();
    }

    private static List<Map<String, Object>> generateRecords(final int numberOfRecords) {
        final List<Map<String, Object>> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {

            final Map<String, Object> eventData = new HashMap<>();

            eventData.put("name", "Person" + rows);
            eventData.put("age", rows);
            eventData.put("myLong", (long) rows + (long) Integer.MAX_VALUE);
            eventData.put("myFloat", rows * 1.5f);
            eventData.put("myDouble", rows * 1.89d);
            eventData.put("myArray", List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
            final Map<String, Object> nestedRecord = new HashMap<>();
            nestedRecord.put("firstFieldInNestedRecord", "testString" + rows);
            nestedRecord.put("secondFieldInNestedRecord", rows);
            eventData.put("nestedRecord", nestedRecord);
            recordList.add(eventData);
        }
        return recordList;
    }

    private static List<Map<String, Object>> generateEmptyRecords(final int numberOfRecords) {
        return IntStream.range(0, numberOfRecords)
                .mapToObj(i -> Collections.<String, Object>emptyMap())
                .collect(Collectors.toList());
    }

    private static Schema createStandardSchema() {
        return createStandardSchema(false);
    }

    private static Schema createStandardSchemaNullable() {
        return createStandardSchema(true);
    }

    private static Schema createStandardSchema(
            final boolean useNullable) {
        final Function<SchemaBuilder.FieldTypeBuilder<Schema>, SchemaBuilder.BaseFieldTypeBuilder<Schema>> typeModifier;
        if(useNullable) {
            typeModifier = SchemaBuilder.FieldTypeBuilder::nullable;
        } else {
            typeModifier = schemaFieldTypeBuilder -> schemaFieldTypeBuilder;
        }
        SchemaBuilder.FieldAssembler<Schema> assembler = SchemaBuilder.record("Person")
                .fields();
        assembler = typeModifier.apply(assembler.name("name").type()).stringType().noDefault();
        assembler = typeModifier.apply(assembler.name("age").type()).intType().noDefault();
        assembler = typeModifier.apply(assembler.name("myLong").type()).longType().noDefault();
        assembler = typeModifier.apply(assembler.name("myFloat").type()).floatType().noDefault();
        assembler = typeModifier.apply(assembler.name("myDouble").type()).doubleType().noDefault();
        assembler = typeModifier.apply(assembler.name("myArray").type()).array().items().stringType().noDefault();
        final Schema innerSchema = createStandardInnerSchemaForNestedRecord(useNullable, typeModifier);
        assembler = assembler.name("nestedRecord").type(innerSchema).noDefault();

        return assembler.endRecord();
    }

    private static Schema createStandardInnerSchemaForNestedRecord(
            boolean useNullable, final Function<SchemaBuilder.FieldTypeBuilder<Schema>, SchemaBuilder.BaseFieldTypeBuilder<Schema>> typeModifier) {
        SchemaBuilder.RecordBuilder<Schema> nestedRecord;
        if(useNullable) {
            nestedRecord = SchemaBuilder.nullable().record("nestedRecord");
        } else {
            nestedRecord = SchemaBuilder.record("nestedRecord");
        }
        SchemaBuilder.FieldAssembler<Schema> assembler = nestedRecord.fields();
        assembler = typeModifier.apply(assembler.name("firstFieldInNestedRecord").type()).stringType().noDefault();
        assembler = typeModifier.apply(assembler.name("secondFieldInNestedRecord").type()).intType().noDefault();
        return assembler.endRecord();
    }

    private static Object decodeOutputIfEncoded(Object encodedActualOutput) {
        if (encodedActualOutput instanceof Utf8) {
            byte[] utf8Bytes = encodedActualOutput.toString().getBytes(StandardCharsets.UTF_8);
            return new String(utf8Bytes, StandardCharsets.UTF_8);
        } else {
            return encodedActualOutput;
        }
    }

    private static List<GenericRecord> createAvroRecordsList(ByteArrayOutputStream outputStream) throws IOException {
        final byte[] avroData = outputStream.toByteArray();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(avroData);
        DataFileStream<GenericRecord> stream = new DataFileStream<GenericRecord>(byteArrayInputStream, new GenericDatumReader<>());
        List<GenericRecord> actualRecords = new ArrayList<>();

        while (stream.hasNext()) {
            GenericRecord avroRecord = stream.next();
            actualRecords.add(avroRecord);
        }
        return actualRecords;
    }

    private static Map<String, Object> convertRecordToMap(GenericRecord nestedRecord) throws Exception {
        final Map<String, Object> eventData = new HashMap<>();
        for (Schema.Field field : nestedRecord.getSchema().getFields()) {
            Object value = decodeOutputIfEncoded(nestedRecord.get(field.name()));
            eventData.put(field.name(), value);
        }
        return eventData;
    }
}
