/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.newline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;


public class NewlineDelimitedOutputCodecTest {
    private ByteArrayOutputStream outputStream;

    private static NewlineDelimitedOutputConfig config;

    private static int numberOfRecords;
    private static final String MESSAGE_FIELD_NAME = "message";
    private static final String TEST_HEADER = "Header";
    private static final String REGEX = "\\r?\\n";
    private static ObjectMapper objectMapper = new ObjectMapper();
    private NewlineDelimitedOutputCodec createObjectUnderTest() {
        String headerDestination= TEST_HEADER;
        config=new NewlineDelimitedOutputConfig();
        config.setHeaderDestination(headerDestination);
        return new NewlineDelimitedOutputCodec(config);
    }
    @ParameterizedTest
    @ValueSource(ints = {1, 3, 10, 100})
    void test_happy_case(final int numberOfRecords) throws IOException {
        this.numberOfRecords = numberOfRecords;
        NewlineDelimitedOutputCodec newlineDelimitedOutputCodec=createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        newlineDelimitedOutputCodec.start(outputStream);
        for (int index=0; index < numberOfRecords; index++) {
            final Event event = (Event) getRecord(index).getData();
            newlineDelimitedOutputCodec.writeEvent(event, outputStream);
        }
        newlineDelimitedOutputCodec.complete(outputStream);
        byte[] byteArray = outputStream.toByteArray();
        String jsonString = null;
        try {
            jsonString = new String(byteArray, StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
        }
        int index=0;
        List<HashMap> expectedRecords = generateRecords(numberOfRecords);
        String[] jsonObjects = jsonString.split(REGEX);
        for (String jsonObject : jsonObjects) {
            if(index!=0){
                Map actualMap = objectMapper.readValue(jsonObject, Map.class);
                Map expectedMap = (Map) expectedRecords.get(index).get(MESSAGE_FIELD_NAME);
                assertThat(expectedMap, Matchers.equalTo(actualMap));
            }else{
                Object expectedData= expectedRecords.get(0).get(config.getHeaderDestination());
                Object actualData= objectMapper.readValue(jsonObject, Map.class).get(TEST_HEADER);
                assertThat(expectedData, Matchers.equalTo(actualData));
            }
            index++;
        }
    }
    private static Record getRecord(int index){
        List<HashMap> recordList=generateRecords(numberOfRecords);
        final Event event = JacksonLog.builder().withData(recordList.get(index)).build();
        return new Record<>(event);
    }
    private static List<HashMap> generateRecords(int numberOfRecords) {

        List<HashMap> recordList = new ArrayList<>();
        for(int rows = 0; rows < numberOfRecords; rows++){
            HashMap ndjsonRecord = new HashMap<>();
            if(rows==0){
                ndjsonRecord.put(config.getHeaderDestination(),"[Name, Age]");
            }
            else{
                HashMap<String, Object> eventData = new HashMap<>();
                eventData.put("name", "Person"+rows);
                eventData.put("age", rows);
                ndjsonRecord.put(MESSAGE_FIELD_NAME, eventData);
            }
            recordList.add((ndjsonRecord));
        }
        return recordList;
    }
}