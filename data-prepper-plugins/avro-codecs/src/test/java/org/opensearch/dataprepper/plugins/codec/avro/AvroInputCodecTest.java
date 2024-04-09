/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.LogEventBuilder;
import org.opensearch.dataprepper.model.io.InputFile;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.NoneDecompressionEngine;
import org.opensearch.dataprepper.plugins.fs.LocalInputFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
public class AvroInputCodecTest {

    private static final int NUM_RECORDS = 100;

    private static final String FILE_NAME = "avro-test";

    private static final String FILE_SUFFIX = ".avro";

    private static int index;

    private static final String INVALID_AVRO_INPUT_STREAM = "Invalid Avro Input Stream";

    @Mock
    private Consumer<Record<Event>> eventConsumer;

    private AvroInputCodec avroInputCodec;

    private static InputFile inputFile;

    private static File testDataFile;

    private AvroInputCodec createObjectUnderTest(){
        return new AvroInputCodec(TestEventFactory.getTestEventFactory());
    }

    @BeforeAll
    public static void setUpAll() throws IOException {
        testDataFile = File.createTempFile(FILE_NAME + "-", FILE_SUFFIX);
        testDataFile.deleteOnExit();
        inputFile = writeRandomAvroData(testDataFile);
    }

    @Test
    public void test_when_nullInputStream_then_throwsException(){
        avroInputCodec=new AvroInputCodec(TestEventFactory.getTestEventFactory());
        Consumer<Record<Event>> eventConsumer = mock(Consumer.class);
        assertThrows(NullPointerException.class,()->
                avroInputCodec.parse((InputStream) null, eventConsumer));

        verifyNoInteractions(eventConsumer);

    }

    @Test
    public void test_when_nullInputFile_then_throwsException(){
        avroInputCodec=new AvroInputCodec(TestEventFactory.getTestEventFactory());
        Consumer<Record<Event>> eventConsumer = mock(Consumer.class);
        assertThrows(NullPointerException.class,()->
                avroInputCodec.parse((InputFile) null, new NoneDecompressionEngine(), eventConsumer));

        verifyNoInteractions(eventConsumer);

    }

    @Test
    public void parse_with_Invalid_InputStream_then_catches_exception()  {
        avroInputCodec=createObjectUnderTest();
        Consumer<Record<Event>> eventConsumer = mock(Consumer.class);
        Assertions.assertDoesNotThrow(()->
                avroInputCodec.parse(createInvalidAvroStream(), eventConsumer));
    }

    @Test
    public void parse_with_null_Consumer_throws() {
        avroInputCodec=createObjectUnderTest();

        final InputStream inputStream = mock(InputStream.class);
        assertThrows(NullPointerException.class,()->
                avroInputCodec.parse(inputStream,null));

        verifyNoInteractions(inputStream);

        final InputFile inputFile = mock(InputFile.class);
        assertThrows(NullPointerException.class,()->
                avroInputCodec.parse(inputFile, new NoneDecompressionEngine(), null));

        verifyNoInteractions(inputFile);
    }

    @Test
    public void parse_with_empty_InputStream_does_not_call_Consumer() throws IOException{
        avroInputCodec=createObjectUnderTest();
        Consumer<Record<Event>> eventConsumer = mock(Consumer.class);
        final ByteArrayInputStream emptyInputStream = new ByteArrayInputStream(new byte[]{});
        avroInputCodec.parse(emptyInputStream, eventConsumer);
        verifyNoInteractions(eventConsumer);
    }

    @Test
    public void test_HappyCaseAvroInputStream_then_callsConsumerWithParsedEvents() throws Exception {

        avroInputCodec = createObjectUnderTest();
        InputStream inputStream = inputFile.newStream();

        try {
            avroInputCodec.parse(inputStream, eventConsumer);

            final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
            verify(eventConsumer, times(NUM_RECORDS)).accept(recordArgumentCaptor.capture());
            final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

            assertRecordsCorrect(actualRecords);
        } finally {
            inputStream.close();
        }
    }

    @Test
    public void test_HappyCaseAvroInputFile_then_callsConsumerWithParsedEvents() throws Exception {

        avroInputCodec = createObjectUnderTest();

        avroInputCodec.parse(inputFile, new NoneDecompressionEngine(), eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(NUM_RECORDS)).accept(recordArgumentCaptor.capture());
        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();

        assertRecordsCorrect(actualRecords);
    }

    private static void assertRecordsCorrect(final List<Record<Event>> records) {
        assertThat(records.size(), equalTo(NUM_RECORDS));
        index = 0;
        for (final Record<Event> actualRecord : records) {

            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(),notNullValue());
            assertThat(actualRecord.getData().getMetadata(),notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            Map<String,Object> expectedMap=new HashMap<>();
            GenericRecord record=generateRecords(parseSchema()).get(index);
            for(Schema.Field field:record.getSchema().getFields()){
                expectedMap.put(field.name(),record.get(field.name()));
            }

            for(String key: expectedMap.keySet()){
                Object actualRecordValue=actualRecord.getData().toMap().get(key);
                if(!(actualRecordValue instanceof Map))
                assertThat(actualRecord.getData().toMap().get(key), equalTo(expectedMap.get(key)));
                else{
                    GenericRecord expectedInnerRecord= (GenericRecord) expectedMap.get(key);
                    Schema innerSchema=expectedInnerRecord.getSchema();
                     for(Schema.Field innerField : innerSchema.getFields()){
                    assertThat(((Map)actualRecordValue).get(innerField.name()),equalTo(expectedInnerRecord.get(innerField.name())));
                        }
                    }
            }
            index++;
        }
    }

    private static Event getEvent(int index){
        List<GenericRecord> recordList = generateRecords(parseSchema());
        GenericRecord record = recordList.get(index);
        Schema schema = parseSchema();
        final Map<String, Object> eventData = new HashMap<>();
        for(Schema.Field field : schema.getFields()) {

            eventData.put(field.name(), record.get(field.name()));

        }
        final Event event = TestEventFactory.getTestEventFactory().eventBuilder(LogEventBuilder.class).withData(eventData).build();
        return event;
    }

    private static InputFile writeRandomAvroData(final File file) throws IOException {
        Schema schema = parseSchema();
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter =new DataFileWriter<>(datumWriter);

        List<GenericRecord> recordList = generateRecords(schema);

        final OutputStream outputStream = new FileOutputStream(file);

        dataFileWriter.create(schema, outputStream);

        for(GenericRecord record: recordList) {
            dataFileWriter.append(record);
        }
        dataFileWriter.close();

        return new LocalInputFile(file);
    }

    private static List<GenericRecord> generateRecords(Schema schema) {

        List<GenericRecord> recordList = new ArrayList<>();

        for(int rows = 0; rows < NUM_RECORDS; rows++){

            GenericRecord record = new GenericData.Record(schema);
            GenericRecord innerRecord = new GenericData.Record(parseInnerSchemaForNestedRecord());
            innerRecord.put("firstFieldInNestedRecord", "testString"+rows);
            innerRecord.put("secondFieldInNestedRecord", rows);

            record.put("name", "Person"+rows);
            record.put("age", rows);
            record.put("nestedRecord", innerRecord);
            recordList.add((record));

        }

        return recordList;

    }

    private static Schema  parseSchema() {

        Schema innerSchema=parseInnerSchemaForNestedRecord();
        return SchemaBuilder.record("Person")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("age").type().intType().noDefault()
                .name("nestedRecord").type(innerSchema).noDefault()
                .endRecord();

    }

    private static Schema parseInnerSchemaForNestedRecord(){
        return SchemaBuilder
                .record("InnerRecord")
                .fields()
                .name("firstFieldInNestedRecord")
                .type(Schema.create(Schema.Type.STRING))
                .noDefault()
                .name("secondFieldInNestedRecord")
                .type(Schema.create(Schema.Type.INT))
                .noDefault()
                .endRecord();
    }

    private static InputStream createInvalidAvroStream() {
        return  new ByteArrayInputStream(INVALID_AVRO_INPUT_STREAM.getBytes());
    }

}
