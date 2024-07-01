/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.io.InputFile;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.NoneDecompressionEngine;
import org.opensearch.dataprepper.plugins.fs.LocalInputFile;
import org.opensearch.dataprepper.plugins.fs.LocalOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.temporal.JulianFields;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.parquet.avro.AvroWriteSupport.WRITE_FIXED_AS_INT96;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.opensearch.dataprepper.plugins.codec.parquet.ParquetInputCodec.EVENT_TYPE;
import static org.opensearch.dataprepper.plugins.codec.parquet.ParquetInputCodec.FILE_PREFIX;
import static org.opensearch.dataprepper.plugins.codec.parquet.ParquetInputCodec.FILE_SUFFIX;

public class ParquetInputCodecTest {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetInputCodecTest.class);

    private static final String SCHEMA_JSON =
            "{\"namespace\": \"org.example.test\"," +
                    " \"type\": \"record\"," +
                    " \"name\": \"TestMessage\"," +
                    " \"fields\": [" +
                    "     {\"name\": \"id\", \"type\": \"string\"}," +
                    "     {\"name\": \"value\", \"type\": \"int\"}," +
                    "     {\"name\": \"alternateIds\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}," +
                    "     {\"name\": \"metadata\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}," +
                    "     {\"name\": \"lastUpdated\", \"type\": \"long\", \"logicalType\": \"timestamp-millis\"}" +
                    " ]}";
    private static EventFactory testEventFactory;

    private ParquetInputCodec parquetInputCodec;
    private Consumer<Record<Event>> mockConsumer;
    private ParquetReader<GenericRecord> mockReader;
    private static File testDataFile;

    @BeforeAll
    public static void setUpAll() throws IOException {
        testDataFile = File.createTempFile(FILE_PREFIX + "-", FILE_SUFFIX);
        testDataFile.deleteOnExit();
        generateTestData(testDataFile);
        testEventFactory = TestEventFactory.getTestEventFactory();
    }

    private static ParquetInputCodec createObjectUnderTest() {
        return new ParquetInputCodec(testEventFactory);
    }

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void setUp() throws IOException {
        parquetInputCodec = createObjectUnderTest();
        mockConsumer = Mockito.mock(Consumer.class);
        mockReader = Mockito.mock(ParquetReader.class);
    }

    @Test
    public void test_when_nullInputStream_then_throwsException(){
        parquetInputCodec = createObjectUnderTest();
        Consumer<Record<Event>> eventConsumer = mock(Consumer.class);
        assertThrows(NullPointerException.class,()->
                parquetInputCodec.parse((InputStream) null, eventConsumer));

        verifyNoInteractions(eventConsumer);
    }

    @Test
    public void test_when_InputStreamNullConfig_then_throwsException(){
        parquetInputCodec = createObjectUnderTest();
        InputStream inputStream = mock(InputStream.class);
        assertThrows(NullPointerException.class,()->
                parquetInputCodec.parse(inputStream, null));

        verifyNoInteractions(inputStream);
    }

    @Test
    public void test_when_nullInputFile_then_throwsException(){
        parquetInputCodec = createObjectUnderTest();
        Consumer<Record<Event>> eventConsumer = mock(Consumer.class);
        assertThrows(NullPointerException.class,()->
                parquetInputCodec.parse((InputFile) null, new NoneDecompressionEngine(), eventConsumer));

        verifyNoInteractions(eventConsumer);
    }

    @Test
    public void test_when_InputFileNullConfig_then_throwsException(){
        parquetInputCodec = createObjectUnderTest();
        InputFile inputFile = mock(InputFile.class);
        assertThrows(NullPointerException.class,()->
                parquetInputCodec.parse(inputFile, new NoneDecompressionEngine(),null));

        verifyNoInteractions(inputFile);
    }

    @Test
    public void parseInputStream_parsesCorrectly() throws IOException {
        InputStream targetStream = new FileInputStream(testDataFile);

        parquetInputCodec.parse(targetStream, mockConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(mockConsumer, times(10)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();
        assertRecordsCorrect(actualRecords);
    }


    @Test
    public void parseInputFile_parsesCorrectly() throws IOException {
        InputFile inputFile = new LocalInputFile(testDataFile);

        parquetInputCodec.parse(inputFile, new NoneDecompressionEngine(), mockConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(mockConsumer, times(10)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();
        assertRecordsCorrect(actualRecords);
    }

    @Test
    public void parseInputStream_parsesCorrectly_with_int96() throws IOException {
        final File testDataFile = File.createTempFile(FILE_PREFIX + "-int96-", FILE_SUFFIX);
        testDataFile.deleteOnExit();
        generateTestDataInt96(testDataFile);
        InputStream targetStream = new FileInputStream(testDataFile);

        parquetInputCodec.parse(targetStream, mockConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(mockConsumer, times(10)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();
        assertThat(actualRecords.size(), equalTo(10));
    }

    @Test
    public void parseInputFile_snappyInputFile() throws IOException, URISyntaxException {
        URL resource = getClass().getClassLoader().getResource("sample.snappy.parquet");
        InputFile inputFile = new LocalInputFile(Paths.get(resource.toURI()).toFile());

        parquetInputCodec.parse(inputFile, new NoneDecompressionEngine(), mockConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(mockConsumer, times(100)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();
        for (final Record<Event> record: actualRecords) {
            final String arch = record.getData().get("architecture", String.class);
            assertThat(arch, startsWith("x86"));
        }
    }

    @Test
    public void parseInputFile_testParquetFile() throws IOException, URISyntaxException {
        URL resource = getClass().getClassLoader().getResource("test-parquet.parquet");
        InputFile inputFile = new LocalInputFile(Paths.get(resource.toURI()).toFile());

        parquetInputCodec.parse(inputFile, new NoneDecompressionEngine(), mockConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(mockConsumer, times(100)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();
        for (int i = 1; i <= actualRecords.size(); i++) {
            final Record<Event> record = actualRecords.get(i - 1);
            LOG.info("record={}", record.toString());
            final String name = record.getData().get("name", String.class);
            final int age = record.getData().get("age", Integer.class);

            assertThat(name, equalTo(String.valueOf(i) + "testString"));
            assertThat(age, equalTo(i));
        }
    }

    private static void generateTestData(final File file) throws IOException {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);

        final ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(file))
                .withSchema(schema)
                .withConf(new PlainParquetConfiguration())
                .withEncryption(null)
                .build();

        for (int i = 0; i < 10; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("id", "id" + i);
            record.put("value", i);
            record.put("alternateIds", Arrays.asList("altid1", "altid2"));
            record.put("metadata", Collections.singletonMap("key", "value"));
            record.put("lastUpdated", 1684509331977L);

            writer.write(record);
        }
        writer.close();
    }

    /**
     * Generates a Parquet file with INT96 data. This must use the example
     * schema rather than Avro, or it would not correctly reproduce possible INT96
     * error.
     *
     * @param file The file for Parquet
     */
    private static void generateTestDataInt96(final File file) throws IOException {
        final MessageType schema = new MessageType("test", List.of(
                new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT96, "my_timestamp_value")
        ));
        final PlainParquetConfiguration conf = new PlainParquetConfiguration();
        conf.setStrings(WRITE_FIXED_AS_INT96,  "my_timestamp_value");
        conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());
        final ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withConf(conf)
                .withEncryption(null)
                .build();

        for (int i = 0; i < 10; i++) {
            final Group group = new SimpleGroup(schema);
            group.add("my_timestamp_value", createInt96());

            writer.write(group);
        }
        writer.close();
    }

    private void assertRecordsCorrect(final List<Record<Event>> records) {
        assertThat(records.size(), equalTo(10));
        for (int i = 0; i < 10; i++) {
            final Record<Event> record = records.get(i);
            final String id = record.getData().get("id", String.class);
            final int value = record.getData().get("value", Integer.class);
            final List<String> alternateIds = (List<String>) record.getData().get("alternateIds", List.class);
            final Map<String, String> metadata = (Map<String, String>) record.getData().get("metadata", Map.class);
            final long lastUpdated = record.getData().get("lastUpdated", Long.class);

            assertThat(id, equalTo("id" + i));
            assertThat(value, equalTo(i));
            assertThat(alternateIds, containsInAnyOrder("altid1", "altid2"));
            assertThat(metadata.get("key"), equalTo("value"));
            assertThat(lastUpdated, equalTo(1684509331977L));

            assertThat(record.getData().getMetadata(), notNullValue());
            assertThat(record.getData().getMetadata().getEventType(), equalTo(EVENT_TYPE));
        }
    }

    private static NanoTime createInt96() {
        return new NanoTime((int) OffsetDateTime.now().getLong(JulianFields.JULIAN_DAY), System.nanoTime());
    }
}

