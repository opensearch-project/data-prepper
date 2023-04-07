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
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
public class AvroInputCodecTest {

    private static final String FILE_NAME = "avro-test";

    private static final String FILE_SUFFIX = ".avro";

    private static int index;

    private static int numberOfRecords;

    private static final String INVALID_AVRO_INPUT_STREAM = "Invalid Avro Input Stream";

    @Mock
    private Consumer<Record<Event>> eventConsumer;

    private AvroInputCodec avroInputCodec;

    private static FileInputStream fileInputStream;

    @TempDir
    private static java.nio.file.Path path;

    private AvroInputCodec createObjectUnderTest(){
        return new AvroInputCodec();
    }

    @Test
    public void test_when_nullInputStream_then_throwsException(){
        avroInputCodec=new AvroInputCodec();
        Consumer<Record<Event>> eventConsumer = mock(Consumer.class);
        assertThrows(NullPointerException.class,()->
                avroInputCodec.parse(null, eventConsumer));

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

    }

    @Test
    public void parse_with_empty_InputStream_does_not_call_Consumer() throws IOException{
        avroInputCodec=createObjectUnderTest();
        Consumer<Record<Event>> eventConsumer = mock(Consumer.class);
        final ByteArrayInputStream emptyInputStream=new ByteArrayInputStream(new byte[]{});
        avroInputCodec.parse(emptyInputStream, eventConsumer);
        verifyNoInteractions(eventConsumer);
    }

    @ParameterizedTest
    @ValueSource(ints={1,10,100,1000})
    public void test_HappyCaseAvroInputStream_then_callsConsumerWithParsedEvents(final int numberOfRecords) throws Exception {

        AvroInputCodecTest.numberOfRecords =numberOfRecords;
        avroInputCodec=createObjectUnderTest();
        InputStream inputStream=createRandomAvroStream(numberOfRecords);

        avroInputCodec.parse(inputStream,eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfRecords)).accept(recordArgumentCaptor.capture());
        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();
        assertThat(actualRecords.size(), equalTo(numberOfRecords));
        index=0;
        for (final Record<Event> actualRecord : actualRecords) {

            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(),notNullValue());
            assertThat(actualRecord.getData().getMetadata(),notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));

            Map expectedMap=getEvent(index).toMap();
            assertThat(actualRecord.getData().toMap(), equalTo(expectedMap));

            for(Object key: actualRecord.getData().toMap().keySet()){
                Object decodedOutput = decodeOutputIfEncoded(actualRecord.getData().toMap() , key);
                Object expectedOutput = getEvent(index).toMap().get(key.toString());
                assertThat(decodedOutput, equalTo(expectedOutput));
            }
            index++;
        }
        fileInputStream.close();
        Files.delete(path);

    }

    private static Object decodeOutputIfEncoded(Map encodedOutput, Object key){
        try{
            JSONObject outputJson = new JSONObject(encodedOutput);
            Map innerJson= (Map) outputJson.get(key.toString());
            byte[] encodedString=(byte[]) innerJson.get("bytes");
            return new String(encodedString, StandardCharsets.UTF_8);

        }catch (Exception e){
            return encodedOutput.get(key);
        }
    }

    private static Event getEvent(int index){
        List<GenericRecord> recordList=generateRecords(parseSchema(),numberOfRecords);
        GenericRecord record=recordList.get(index);
        Schema schema=parseSchema();
        final Map<String, Object> eventData = new HashMap<>();
        for(Schema.Field field : schema.getFields()) {

            eventData.put(field.name(), record.get(field.name()));

        }
        final Event event = JacksonLog.builder().withData(eventData).build();
        return event;
    }


    private static InputStream createRandomAvroStream(int numberOfRecords) throws IOException{

        Files.deleteIfExists(Path.of(FILE_NAME + FILE_SUFFIX));
        Schema schema=parseSchema();
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter=new DataFileWriter<>(datumWriter);

        List<GenericRecord> recordList = generateRecords(schema, numberOfRecords);

        File avroTestFile=new File(FILE_NAME+FILE_SUFFIX);
        path= Paths.get(FILE_NAME+FILE_SUFFIX);
        dataFileWriter.create(schema, avroTestFile);

        for(GenericRecord record: recordList) {
            dataFileWriter.append(record);
        }
        dataFileWriter.close();

        fileInputStream = new FileInputStream(path.toString());
        return fileInputStream;

    }

    private static List<GenericRecord> generateRecords(Schema schema, int numberOfRecords) {

        List<GenericRecord> recordList = new ArrayList<>();

        for(int rows = 0; rows < numberOfRecords; rows++){

            GenericRecord record = new GenericData.Record(schema);

            record.put("name", "Person"+rows);
            record.put("age", rows);
            recordList.add((record));

        }

        return recordList;

    }

    private static Schema  parseSchema() {

        return SchemaBuilder.record("Person")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("age").type().intType().noDefault()
                .endRecord();

    }

    private static InputStream createInvalidAvroStream() {
        return  new ByteArrayInputStream(INVALID_AVRO_INPUT_STREAM.getBytes());
    }

}
