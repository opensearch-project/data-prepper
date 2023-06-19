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
    void test_happy_case(final int numberOfRecords) throws IOException {
        this.numberOfRecords = numberOfRecords;
        AvroOutputCodec avroOutputCodec = createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        avroOutputCodec.start(outputStream);
        for (int index = 0; index < numberOfRecords; index++) {
            final Event event = (Event) getRecord(index).getData();
            avroOutputCodec.writeEvent(event, outputStream);
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
                Object decodedActualOutput = decodeOutputIfEncoded(actualRecord.get(field.name()));
                actualMap.put(field.name(), decodedActualOutput);
            }
            assertThat(expectedMap, Matchers.equalTo(actualMap));
            index++;
        }
    }


    private static Record getRecord(int index) {
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
            recordList.add((eventData));

        }
        return recordList;
    }

    static Schema parseSchema() {
        return SchemaBuilder.record("Person")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("age").type().intType().noDefault()
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
        Schema schema = stream.getSchema();
        List<GenericRecord> actualRecords = new ArrayList<>();

        while (stream.hasNext()) {
            GenericRecord avroRecord = stream.next();
            actualRecords.add(avroRecord);
        }
        return actualRecords;
    }
}
