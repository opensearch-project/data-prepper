/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.fs.LocalFilePositionOutputStream;
import org.opensearch.dataprepper.plugins.sink.s3.S3OutputCodecContext;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
public class ParquetOutputCodecTest {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetOutputCodecTest.class);
    private static final String FILE_NAME = "parquet-data.parquet";
    private ParquetOutputCodecConfig config;
    @Mock
    private S3OutputCodecContext codecContext;

    @TempDir
    private File tempDirectory;

    @BeforeEach
    void setUp() {
        config = new ParquetOutputCodecConfig();
    }

    private ParquetOutputCodec createObjectUnderTest() {
        return new ParquetOutputCodec(config);
    }

    @Test
    void constructor_throws_if_schema_is_invalid() {
        String invalidSchema = createStandardSchema().toString().replaceAll(",", ";");
        config.setSchema(invalidSchema);

        RuntimeException actualException = assertThrows(RuntimeException.class, this::createObjectUnderTest);

        assertThat(actualException.getMessage(), notNullValue());
        assertThat(actualException.getMessage(), containsString("was expecting comma"));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_happy_case(final int numberOfRecords) throws Exception {
        config.setSchema(createStandardSchema().toString());
        when(codecContext.getCompressionOption()).thenReturn(CompressionOption.NONE);
        ParquetOutputCodec parquetOutputCodec = createObjectUnderTest();
        final File tempFile = new File(tempDirectory, FILE_NAME);
        LocalFilePositionOutputStream outputStream = LocalFilePositionOutputStream.create(tempFile);
        parquetOutputCodec.start(outputStream, null, codecContext);
        List<Map<String, Object>> inputMaps = generateRecords(numberOfRecords);
        for (Map<String, Object> inputMap : inputMaps) {
            final Event event = createEventRecord(inputMap);
            parquetOutputCodec.writeEvent(event, outputStream);
        }
        parquetOutputCodec.complete(outputStream);
        List<Map<String, Object>> actualRecords = createParquetRecordsList(new FileInputStream(tempFile));
        int index = 0;
        assertThat(inputMaps.size(), equalTo(actualRecords.size()));
        for (final Map<String, Object> actualMap : actualRecords) {
            assertThat(actualMap, notNullValue());
            Map expectedMap = inputMaps.get(index);
            assertThat(expectedMap, Matchers.equalTo(actualMap));
            index++;
        }
        outputStream.close();
        tempFile.delete();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_happy_case_nullable_records(final int numberOfRecords) throws Exception {
        config.setSchema(createStandardSchemaNullable().toString());
        when(codecContext.getCompressionOption()).thenReturn(CompressionOption.NONE);
        ParquetOutputCodec parquetOutputCodec = createObjectUnderTest();
        final File tempFile = new File(tempDirectory, FILE_NAME);
        LocalFilePositionOutputStream outputStream = LocalFilePositionOutputStream.create(tempFile);
        parquetOutputCodec.start(outputStream, null, codecContext);
        List<Map<String, Object>> inputMaps = generateRecords(numberOfRecords);
        for (Map<String, Object> inputMap : inputMaps) {
            final Event event = createEventRecord(inputMap);
            parquetOutputCodec.writeEvent(event, outputStream);
        }
        parquetOutputCodec.complete(outputStream);
        List<Map<String, Object>> actualRecords = createParquetRecordsList(new FileInputStream(tempFile));
        int index = 0;
        assertThat(inputMaps.size(), equalTo(actualRecords.size()));
        for (final Map<String, Object> actualMap : actualRecords) {
            assertThat(actualMap, notNullValue());
            Map expectedMap = inputMaps.get(index);
            assertThat(expectedMap, Matchers.equalTo(actualMap));
            index++;
        }
        outputStream.close();
        tempFile.delete();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_happy_case_nullable_records_with_empty_maps(final int numberOfRecords) throws Exception {
        config.setSchema(createStandardSchemaNullable().toString());
        when(codecContext.getCompressionOption()).thenReturn(CompressionOption.NONE);
        ParquetOutputCodec parquetOutputCodec = createObjectUnderTest();
        final File tempFile = new File(tempDirectory, FILE_NAME);
        LocalFilePositionOutputStream outputStream = LocalFilePositionOutputStream.create(tempFile);
        parquetOutputCodec.start(outputStream, null, codecContext);
        List<Map<String, Object>> inputMaps = generateEmptyRecords(numberOfRecords);
        for (Map<String, Object> inputMap : inputMaps) {
            final Event event = createEventRecord(inputMap);
            parquetOutputCodec.writeEvent(event, outputStream);
        }
        parquetOutputCodec.complete(outputStream);
        List<Map<String, Object>> actualRecords = createParquetRecordsList(new FileInputStream(tempFile));
        int index = 0;
        assertThat(inputMaps.size(), equalTo(actualRecords.size()));
        for (final Map<String, Object> actualMap : actualRecords) {
            assertThat(actualMap, notNullValue());
            Map expectedMap = inputMaps.get(index);
            assertThat(expectedMap, Matchers.equalTo(actualMap));
            index++;
        }
        tempFile.delete();
    }

    @Test
    public void test_getExtension() {
        ParquetOutputCodec parquetOutputCodec = createObjectUnderTest();
        String extension = parquetOutputCodec.getExtension();

        assertThat(extension, equalTo("parquet"));
    }

    @Test
    void writeEvent_includes_record_when_field_does_not_exist_in_user_supplied_schema() throws IOException {
        config.setSchema(createStandardSchema().toString());
        when(codecContext.getCompressionOption()).thenReturn(CompressionOption.NONE);
        final Event eventWithInvalidField = mock(Event.class);
        final String invalidFieldName = UUID.randomUUID().toString();
        Map<String, Object> mapWithInvalid = generateRecords(1).get(0);
        Map<String, Object> mapWithoutInvalid = mapWithInvalid.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        mapWithInvalid.put(invalidFieldName, UUID.randomUUID().toString());
        when(eventWithInvalidField.toMap()).thenReturn(mapWithInvalid);
        final ParquetOutputCodec objectUnderTest = createObjectUnderTest();

        final File tempFile = new File(tempDirectory, FILE_NAME);
        LocalFilePositionOutputStream outputStream = LocalFilePositionOutputStream.create(tempFile);
        objectUnderTest.start(outputStream, null, codecContext);

        objectUnderTest.writeEvent(eventWithInvalidField, outputStream);

        objectUnderTest.complete(outputStream);
        List<Map<String, Object>> actualRecords = createParquetRecordsList(new FileInputStream(tempFile));
        int index = 0;
        for (final Map<String, Object> actualMap : actualRecords) {
            assertThat(actualMap, notNullValue());
            assertThat(mapWithInvalid, not(Matchers.equalTo(actualMap)));
            assertThat(mapWithoutInvalid, Matchers.equalTo(actualMap));
            index++;
        }
    }

    @Test
    void writeEvent_throws_exception_when_field_does_not_exist_in_auto_schema() throws IOException {
        config.setSchema(null);
        when(codecContext.getCompressionOption()).thenReturn(CompressionOption.NONE);
        final Event eventWithInvalidField = mock(Event.class);
        final String invalidFieldName = UUID.randomUUID().toString();
        when(eventWithInvalidField.toMap()).thenReturn(Collections.singletonMap(invalidFieldName, UUID.randomUUID().toString()));
        final ParquetOutputCodec objectUnderTest = createObjectUnderTest();

        final File tempFile = new File(tempDirectory, FILE_NAME);
        LocalFilePositionOutputStream outputStream = LocalFilePositionOutputStream.create(tempFile);
        objectUnderTest.start(outputStream, createEventRecord(generateRecords(1).get(0)), codecContext);
        outputStream.close();
        final RuntimeException actualException = assertThrows(RuntimeException.class, () -> objectUnderTest.writeEvent(eventWithInvalidField, outputStream));

        assertThat(actualException.getMessage(), notNullValue());
        assertThat(actualException.getMessage(), containsString(invalidFieldName));
    }

    @Test
    void exception_in_start_should_not_cause_null_pointer_exception_when_complete() throws IOException {
        final ParquetOutputCodec objectUnderTest = createObjectUnderTest();

        final File tempFile = new File(tempDirectory, FILE_NAME);
        LocalFilePositionOutputStream outputStream = LocalFilePositionOutputStream.create(tempFile);
        assertThrows(RuntimeException.class, () -> objectUnderTest.start(null, createEventRecord(generateRecords(1).get(0)), codecContext));

        // Calling complete now should not throw
        objectUnderTest.complete(outputStream);
    }

    @Test
    void getSize_returns_0_after_construction() {
        config.setSchema(createStandardSchema().toString());

        Optional<Long> actualSizeOptional = createObjectUnderTest().getSize();

        assertThat(actualSizeOptional, notNullValue());
        assertThat(actualSizeOptional.isPresent(), equalTo(true));
        assertThat(actualSizeOptional.get(), equalTo(0L));
    }

    @Test
    void getSize_returns_0_when_first_started() throws IOException {
        config.setSchema(createStandardSchema().toString());
        when(codecContext.getCompressionOption()).thenReturn(CompressionOption.NONE);

        final File tempFile = new File(tempDirectory, FILE_NAME);
        LocalFilePositionOutputStream outputStream = LocalFilePositionOutputStream.create(tempFile);

        ParquetOutputCodec objectUnderTest = createObjectUnderTest();
        objectUnderTest.start(outputStream, null, codecContext);
        outputStream.close();
        Optional<Long> actualSizeOptional = objectUnderTest.getSize();
        assertThat(actualSizeOptional, notNullValue());
        assertThat(actualSizeOptional.isPresent(), equalTo(true));
        assertThat(actualSizeOptional.get(), equalTo(0L));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10})
    void getSize_returns_non_zero_after_writes(int writeCount) throws IOException {
        config.setSchema(createStandardSchema().toString());
        when(codecContext.getCompressionOption()).thenReturn(CompressionOption.NONE);

        final File tempFile = new File(tempDirectory, FILE_NAME);
        LocalFilePositionOutputStream outputStream = LocalFilePositionOutputStream.create(tempFile);

        List<Map<String, Object>> records = generateRecords(writeCount);

        final long roughMultiplierMin = 100;
        final long roughMultiplierMax = 200;

        ParquetOutputCodec objectUnderTest = createObjectUnderTest();
        objectUnderTest.start(outputStream, null, codecContext);

        for (Map<String, Object> record : records) {
            Event event = createEventRecord(record);
            objectUnderTest.writeEvent(event, outputStream);
        }
        outputStream.close();

        Optional<Long> actualSizeOptional = objectUnderTest.getSize();
        assertThat(actualSizeOptional, notNullValue());
        assertThat(actualSizeOptional.isPresent(), equalTo(true));
        assertThat(actualSizeOptional.get(), greaterThanOrEqualTo(roughMultiplierMin * writeCount));
        assertThat(actualSizeOptional.get(), lessThanOrEqualTo(roughMultiplierMax * writeCount));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10})
    void getSize_returns_empty_after_close(int writeCount) throws IOException {
        config.setSchema(createStandardSchema().toString());
        when(codecContext.getCompressionOption()).thenReturn(CompressionOption.NONE);

        final File tempFile = new File(tempDirectory, FILE_NAME);
        LocalFilePositionOutputStream outputStream = LocalFilePositionOutputStream.create(tempFile);

        List<Map<String, Object>> records = generateRecords(writeCount);

        ParquetOutputCodec objectUnderTest = createObjectUnderTest();
        objectUnderTest.start(outputStream, null, codecContext);

        for (Map<String, Object> record : records) {
            Event event = createEventRecord(record);
            objectUnderTest.writeEvent(event, outputStream);
        }

        objectUnderTest.complete(outputStream);
        outputStream.close();
        Optional<Long> actualSizeOptional = objectUnderTest.getSize();
        assertThat(actualSizeOptional, notNullValue());
        assertThat(actualSizeOptional.isPresent(), equalTo(false));
    }


    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10})
    void getSize_returns_non_zero_after_close_and_new_writes(int writeCount) throws IOException {
        config.setSchema(createStandardSchema().toString());
        when(codecContext.getCompressionOption()).thenReturn(CompressionOption.NONE);

        File tempFile = new File(tempDirectory, FILE_NAME);
        LocalFilePositionOutputStream outputStream = LocalFilePositionOutputStream.create(tempFile);

        List<Map<String, Object>> records = generateRecords(writeCount);

        final long roughMultiplierMin = 100;
        final long roughMultiplierMax = 200;

        ParquetOutputCodec objectUnderTest = createObjectUnderTest();
        objectUnderTest.start(outputStream, null, codecContext);

        for (Map<String, Object> record : records) {
            Event event = createEventRecord(record);
            objectUnderTest.writeEvent(event, outputStream);
        }

        objectUnderTest.complete(outputStream);
        outputStream.close();

        tempFile = new File(tempDirectory, FILE_NAME);
        outputStream = LocalFilePositionOutputStream.create(tempFile);

        objectUnderTest.start(outputStream, null, codecContext);

        for (Map<String, Object> record : records) {
            Event event = createEventRecord(record);
            objectUnderTest.writeEvent(event, outputStream);
        }
        outputStream.close();

        Optional<Long> actualSizeOptional = objectUnderTest.getSize();
        assertThat(actualSizeOptional, notNullValue());
        assertThat(actualSizeOptional.isPresent(), equalTo(true));
        assertThat(actualSizeOptional.get(), greaterThanOrEqualTo(roughMultiplierMin * writeCount));
        assertThat(actualSizeOptional.get(), lessThanOrEqualTo(roughMultiplierMax * writeCount));
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

            ParquetOutputCodec objectUnderTest = createObjectUnderTest();
            assertThrows(InvalidPluginConfigurationException.class, () -> objectUnderTest.validateAgainstCodecContext(codecContext));
        }

        @Test
        void validateAgainstCodecContext_throws_when_user_defined_schema_and_excludeKeys_non_empty() {
            when(codecContext.getExcludeKeys()).thenReturn(keys);

            ParquetOutputCodec objectUnderTest = createObjectUnderTest();
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

    private  List<String> extractStringList(SimpleGroup group, String fieldName) {
        int fieldIndex = group.getType().getFieldIndex(fieldName);
        int repetitionCount = group.getGroup(fieldIndex, 0).getFieldRepetitionCount(0);
        List<String> resultList = new ArrayList<>();
        for (int i = 0; i < repetitionCount; i++) {
            resultList.add(group.getGroup(fieldIndex, 0).getString(0, i));
        }
        return resultList;
    }

    private  Map<String, Object> extractNestedGroup(SimpleGroup group, String fieldName) {

        Map<String, Object> resultMap = new HashMap<>();
        int fieldIndex = group.getType().getFieldIndex(fieldName);
        int f1 = group.getGroup(fieldIndex, 0).getType().getFieldIndex("firstFieldInNestedRecord");
        resultMap.put("firstFieldInNestedRecord", group.getGroup(fieldIndex, 0).getString(f1,0));
        int f2 = group.getGroup(fieldIndex, 0).getType().getFieldIndex("secondFieldInNestedRecord");
        resultMap.put("secondFieldInNestedRecord", group.getGroup(fieldIndex, 0).getInteger(f2,0));

        return resultMap;
    }

    private List<Map<String, Object>> createParquetRecordsList(final InputStream inputStream) throws IOException, RuntimeException {

        final File tempFile = new File(tempDirectory, FILE_NAME);
        Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        List<Map<String, Object>> actualRecordList = new ArrayList<>();
        try (ParquetFileReader parquetFileReader = new ParquetFileReader(HadoopInputFile.fromPath(new Path(tempFile.toURI()), new Configuration()), ParquetReadOptions.builder().build())) {
            final ParquetMetadata footer = parquetFileReader.getFooter();
            final MessageType schema = createdParquetSchema(footer);
            PageReadStore pages;

            while ((pages = parquetFileReader.readNextRowGroup()) != null) {
                final long rows = pages.getRowCount();
                final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int row = 0; row < rows; row++) {
                    final Map<String, Object> eventData = new HashMap<>();
                    final SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                    final GroupType groupType = simpleGroup.getType();


                    for (Type field : schema.getFields()) {
                        Object value;
                        int fieldIndex = groupType.getFieldIndex(field.getName());
			if (simpleGroup.getFieldRepetitionCount(fieldIndex) == 0) {
                            continue;
                        }
                        switch (field.getName()) {
                            case "name": value = simpleGroup.getString(fieldIndex, 0);
                                        break;
                            case "age": value = simpleGroup.getInteger(fieldIndex, 0);
                                        break;
                            case "myLong": value = simpleGroup.getLong(fieldIndex, 0);
                                        break;
                            case "myFloat": value = simpleGroup.getFloat(fieldIndex, 0);
                                        break;
                            case "myDouble": value = simpleGroup.getDouble(fieldIndex, 0);
                                        break;
                            case "myArray": value = extractStringList(simpleGroup, "myArray");
                                        break;
                            case "nestedRecord": value = extractNestedGroup(simpleGroup, "nestedRecord");
                                        break;
                            default: throw new IllegalArgumentException("Unknown field");
                        }
                        eventData.put(field.getName(), value);
                    }
                    actualRecordList.add((HashMap) eventData);
                }
            }
        } catch (Exception parquetException) {
            LOG.error("Failed to parse Parquet", parquetException);
        } finally {
            Files.delete(tempFile.toPath());
        }
        return actualRecordList;
    }

    private MessageType createdParquetSchema(ParquetMetadata parquetMetadata) {
        return parquetMetadata.getFileMetaData().getSchema();
    }
}
