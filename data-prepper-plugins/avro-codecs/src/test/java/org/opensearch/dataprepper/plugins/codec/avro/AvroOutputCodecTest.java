/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class AvroOutputCodecTest {
    private static String expectedSchemaString = "{\"type\":\"record\",\"name\":\"AvroRecords\",\"fields\":[{\"name\"" +
            ":\"name\",\"type\":\"string\"},{\"name\":\"nestedRecord\",\"type\":{\"type\":\"record\",\"name\":" +
            "\"NestedRecord1\",\"fields\":[{\"name\":\"secondFieldInNestedRecord\",\"type\":\"int\"},{\"name\":\"" +
            "firstFieldInNestedRecord\",\"type\":\"string\"}]}},{\"name\":\"age\",\"type\":\"int\"}]}";
    private static final String getExpectedSchemaStringTabular="{\"type\":\"record\",\"name\":\"sesblog\",\"fields\":[{\"name\"" +
            ":\"eventType\",\"type\":\"string\"},{\"name\":\"ews\",\"type\":\"string\"},{\"name\":\"mail\",\"type\":" +
            "{\"type\":\"record\",\"name\":\"sesblog_0\",\"fields\":[{\"name\":\"col1\",\"type\":\"string\"},{\"name\":\"" +
            "innercolName\",\"type\":{\"type\":\"record\",\"name\":\"sesblog_1\",\"fields\":[{\"name\":\"colInner\"," +
            "\"type\":\"string\"}]}}]}},{\"name\":\"collumn2\",\"type\":\"string\"}]}";
    private static final String inputString = "TABLE sesblog (\n" +
            "  eventType string,\n" +
            "  ews string,\n" +
            "  mail struct<col1:string,\n" +
            "              innercolName struct<colInner:string> \n" +
            "              >,\n" +
            "  collumn2 string) ";
    private AvroOutputCodecConfig config;

    private ByteArrayOutputStream outputStream;

    private static int numberOfRecords;

    private AvroOutputCodec createObjectUnderTest() {
        config = new AvroOutputCodecConfig();
        config.setSchema(parseSchema().toString());
        return new AvroOutputCodec(config);
    }


    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_happy_case(final int numberOfRecords) throws Exception {
        this.numberOfRecords = numberOfRecords;
        AvroOutputCodec avroOutputCodec = createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        avroOutputCodec.start(outputStream, null, null);
        for (int index = 0; index < numberOfRecords; index++) {
            final Event event = (Event) getRecord(index).getData();
            avroOutputCodec.writeEvent(event, outputStream, null);
        }
        avroOutputCodec.complete(outputStream);
        List<GenericRecord> actualRecords = createAvroRecordsList(outputStream);
        int index = 0;
        for (final GenericRecord actualRecord : actualRecords) {

            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getSchema(), notNullValue());

            Map expectedMap = generateRecords(numberOfRecords).get(index);
            Map actualMap = new HashMap();
            for (Schema.Field field : actualRecord.getSchema().getFields()) {
                if(actualRecord.get(field.name()) instanceof GenericRecord){
                    GenericRecord nestedRecord = (GenericRecord) actualRecord.get(field.name());
                    actualMap.put(field.name(), convertRecordToMap(nestedRecord));
                }
                else{
                    Object decodedActualOutput = decodeOutputIfEncoded(actualRecord.get(field.name()));
                    actualMap.put(field.name(), decodedActualOutput);
                }
            }
            assertThat(expectedMap, Matchers.equalTo(actualMap));
            index++;
        }
    }
    @Test
    public void testInlineSchemaBuilder() throws IOException {
        Schema expectedSchema = new Schema.Parser().parse(expectedSchemaString);
        AvroOutputCodec avroOutputCodec = createObjectUnderTest();
        numberOfRecords = 1;
        Event event = getRecord(0).getData();
        Schema actualSchema = avroOutputCodec.buildInlineSchemaFromEvent(event, null);
        assertThat(actualSchema, Matchers.equalTo(expectedSchema));
    }


    private static Record<Event> getRecord(int index) {
        List<HashMap> recordList = generateRecords(numberOfRecords);
        final Event event = JacksonLog.builder().withData(recordList.get(index)).build();
        return new Record<>(event);
    }

    private static List<HashMap> generateRecords(int numberOfRecords) {

        List<HashMap> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {

            HashMap<String, Object> eventData = new HashMap<>();

            eventData.put("name", "Person" + rows);
            eventData.put("age", rows);
            HashMap<String, Object> nestedRecord = new HashMap<>();
            nestedRecord.put("firstFieldInNestedRecord", "testString"+rows);
            nestedRecord.put("secondFieldInNestedRecord", rows);
            eventData.put("nestedRecord", nestedRecord);
            recordList.add((eventData));

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
                .record("nestedRecord")
                .fields()
                .name("firstFieldInNestedRecord")
                .type(Schema.create(Schema.Type.STRING))
                .noDefault()
                .name("secondFieldInNestedRecord")
                .type(Schema.create(Schema.Type.INT))
                .noDefault()
                .endRecord();
    }

    private static Object decodeOutputIfEncoded(Object encodedActualOutput) throws UnsupportedEncodingException {
        if(encodedActualOutput instanceof Utf8){
            byte[] utf8Bytes = encodedActualOutput.toString().getBytes("UTF-8");
            return new String(utf8Bytes, "UTF-8");
        }
        else{
            return encodedActualOutput;
        }
    }

    private static List<GenericRecord> createAvroRecordsList(ByteArrayOutputStream outputStream) throws IOException {
        final byte[] avroData = outputStream.toByteArray();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(avroData);
        DataFileStream<GenericRecord> stream = new DataFileStream<GenericRecord>(byteArrayInputStream, new GenericDatumReader<GenericRecord>());
        List<GenericRecord> actualRecords = new ArrayList<>();

        while (stream.hasNext()) {
            GenericRecord avroRecord = stream.next();
            actualRecords.add(avroRecord);
        }
        return actualRecords;
    }
    private static Map<String, Object> convertRecordToMap(GenericRecord nestedRecord) throws Exception {
        final Map<String, Object> eventData = new HashMap<>();
        for(Schema.Field field : nestedRecord.getSchema().getFields()){
            Object value = decodeOutputIfEncoded(nestedRecord.get(field.name()));
            eventData.put(field.name(), value);
        }
        return eventData;
    }

    @Test
    public void testTabularSchemaParser() throws IOException {
        Schema expectedSchema = new Schema.Parser().parse(getExpectedSchemaStringTabular);
        Schema actualSchema=AvroSchemaParserFromTabularFormat.generateSchemaFromTabularString(inputString);
        assertThat(actualSchema, Matchers.equalTo(expectedSchema));
    }
}
