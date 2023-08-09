/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class AvroCodecsIT {
    private static final String FILE_NAME = "avro-test";

    private static final String FILE_SUFFIX = ".avro";

    private static int index;

    private static int numberOfRecords;
    private static FileInputStream fileInputStream;
    @TempDir
    private static java.nio.file.Path path;
    @Mock
    private Consumer<Record<Event>> eventConsumer;
    private AvroInputCodec avroInputCodec;
    private AvroOutputCodecConfig config;

    private static Object decodeOutputIfEncoded(Object encodedActualOutput) throws UnsupportedEncodingException {
        if (encodedActualOutput instanceof Utf8) {
            byte[] utf8Bytes = encodedActualOutput.toString().getBytes(StandardCharsets.UTF_8);
            return new String(utf8Bytes, StandardCharsets.UTF_8);
        } else {
            return encodedActualOutput;
        }
    }

    private static Event getEvent(int index) {
        List<GenericRecord> recordList = generateRecords(parseSchema(), numberOfRecords);
        GenericRecord record = recordList.get(index);
        Schema schema = parseSchema();
        final Map<String, Object> eventData = new HashMap<>();
        for (Schema.Field field : schema.getFields()) {

            eventData.put(field.name(), record.get(field.name()));

        }
        final Event event = JacksonLog.builder().withData(eventData).build();
        return event;
    }

    private static InputStream createRandomAvroStream(int numberOfRecords) throws IOException {

        Files.deleteIfExists(Path.of(FILE_NAME + FILE_SUFFIX));
        Schema schema = parseSchema();
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

        List<GenericRecord> recordList = generateRecords(schema, numberOfRecords);

        File avroTestFile = new File(FILE_NAME + FILE_SUFFIX);
        path = Paths.get(FILE_NAME + FILE_SUFFIX);
        dataFileWriter.create(schema, avroTestFile);

        for (GenericRecord record : recordList) {
            dataFileWriter.append(record);
        }
        dataFileWriter.close();

        fileInputStream = new FileInputStream(path.toString());
        return fileInputStream;

    }

    private static List<GenericRecord> generateRecords(Schema schema, int numberOfRecords) {

        List<GenericRecord> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {

            GenericRecord record = new GenericData.Record(schema);

            record.put("name", "Person" + rows);
            record.put("age", rows);
            recordList.add((record));

        }

        return recordList;

    }

    private static Schema parseSchema() {

        return SchemaBuilder.record("Person")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("age").type().intType().noDefault()
                .endRecord();

    }

    private static List<GenericRecord> createAvroRecordsList(ByteArrayOutputStream outputStream) throws IOException {
        final byte[] avroData = outputStream.toByteArray();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(avroData);
        DataFileStream<GenericRecord> stream = new DataFileStream<GenericRecord>(byteArrayInputStream, new GenericDatumReader<GenericRecord>());
        Schema schema = stream.getSchema();
        List<GenericRecord> actualRecords = new ArrayList<>();

        while (stream.hasNext()) {
            GenericRecord avroRecord = stream.next();
            actualRecords.add(avroRecord);
        }
        return actualRecords;
    }

    private AvroInputCodec createInputCodecObjectUnderTest() {
        return new AvroInputCodec();
    }

    private AvroOutputCodec createOutputCodecObjectUnderTest() {

        config = new AvroOutputCodecConfig();
        config.setSchema(parseSchema().toString());
        return new AvroOutputCodec(config);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 100, 1000})
    public void test_HappyCaseAvroInputStream_then_callsConsumerWithParsedEvents(final int numberOfRecords) throws Exception {

        AvroCodecsIT.numberOfRecords = numberOfRecords;
        avroInputCodec = createInputCodecObjectUnderTest();
        AvroOutputCodec avroOutputCodec = createOutputCodecObjectUnderTest();
        InputStream inputStream = createRandomAvroStream(numberOfRecords);

        avroInputCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfRecords)).accept(recordArgumentCaptor.capture());
        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputCodecContext codecContext = new OutputCodecContext();
        avroOutputCodec.start(outputStream, null, codecContext);
        for (Record<Event> record : actualRecords) {
            avroOutputCodec.writeEvent(record.getData(), outputStream);
        }
        avroOutputCodec.complete(outputStream);
        List<GenericRecord> actualOutputRecords = createAvroRecordsList(outputStream);
        int index = 0;
        for (final GenericRecord actualRecord : actualOutputRecords) {

            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getSchema(), notNullValue());

            Map expectedMap = actualRecords.get(index).getData().toMap();
            Map actualMap = new HashMap();
            for (Schema.Field field : actualRecord.getSchema().getFields()) {
                Object decodedActualOutput = decodeOutputIfEncoded(actualRecord.get(field.name()));
                actualMap.put(field.name(), decodedActualOutput);
            }
            assertThat(expectedMap, Matchers.equalTo(actualMap));
            index++;
        }
        fileInputStream.close();
        Files.delete(path);

    }
}
