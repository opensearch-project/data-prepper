/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.accumulator.Buffer;
import org.opensearch.dataprepper.plugins.accumulator.InMemoryBufferFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SqsSinkBatchEntryTest {
    private Buffer buffer;
    
    private OutputCodec outputCodec;
    
    private OutputCodecContext outputCodecContext;
    
    private String groupId;
    private String deDupId;
    private ObjectMapper objectMapper;
    
    private SqsSinkBatchEntry createObjectUnderTest() {
        return new SqsSinkBatchEntry(buffer, groupId, deDupId, outputCodec, outputCodecContext);
    }

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper();
        InMemoryBufferFactory inMemoryBufferFactory = new InMemoryBufferFactory();
        buffer = inMemoryBufferFactory.getBuffer();
        outputCodec = new JsonOutputCodec(new JsonOutputCodecConfig());
        outputCodecContext = new OutputCodecContext();
        groupId = UUID.randomUUID().toString();
        deDupId = UUID.randomUUID().toString();
    }

    @Test
    void TestBasic() {
        SqsSinkBatchEntry sqsSinkBatchEntry = createObjectUnderTest();
        assertThat(sqsSinkBatchEntry.getBody().length(), equalTo(0));
        assertThat(sqsSinkBatchEntry.getSize(), equalTo(0L));
        assertThat(sqsSinkBatchEntry.getEventCount(), equalTo(0));
        assertThat(sqsSinkBatchEntry.getGroupId(), equalTo(groupId));
        assertThat(sqsSinkBatchEntry.getDedupId(), equalTo(deDupId));
        assertTrue(sqsSinkBatchEntry.getEventHandles().isEmpty());
    }
     
    @Test
    void TestAddingOneEvent() throws Exception {
        SqsSinkBatchEntry sqsSinkBatchEntry = createObjectUnderTest();
        List<Record<Event>> records = getRecordList(1);
        Event event = records.get(0).getData();
        sqsSinkBatchEntry.addEvent(event);
        sqsSinkBatchEntry.complete();
        String expectedBody = "{\"events\":["+event.toJsonString()+"]}";
        assertThat(sqsSinkBatchEntry.getEventCount(), equalTo(1));
        assertThat(sqsSinkBatchEntry.getBody(), equalTo(expectedBody));
        assertThat(sqsSinkBatchEntry.getSize(), equalTo((long)expectedBody.length()));
        assertThat(sqsSinkBatchEntry.getGroupId(), equalTo(groupId));
        assertThat(sqsSinkBatchEntry.getDedupId(), equalTo(deDupId));
        assertThat(sqsSinkBatchEntry.getEventHandles().size(), equalTo(1));
    }
    

    @ParameterizedTest
    @ValueSource(ints = {10, 25, 57, 73})
    void TestAddingMultipleEvents(int numRecords) throws Exception {
        SqsSinkBatchEntry sqsSinkBatchEntry = createObjectUnderTest();
        List<Record<Event>> records = getRecordList(numRecords);
        long expectedSize = "{\"events\":[]}".length();

        for (Record<Event> record: records) {
            Event event = record.getData();
            sqsSinkBatchEntry.addEvent(event);
            expectedSize += event.toJsonString().length();
        }
        // account for commas
        expectedSize += (records.size() - 1);

        sqsSinkBatchEntry.complete();
        final Map<String, Object> expectedMap = new HashMap<>();
        for (int i = 0; i < numRecords; i++) {
            expectedMap.put("Person"+i, Integer.toString(i));
        }
        Map<String, Object> body = objectMapper.readValue(sqsSinkBatchEntry.getBody(), Map.class);
        List<Map<String, Object>> events = (List<Map<String, Object>>) body.get("events");
        assertThat(events.size(), equalTo(numRecords));
        
        for (int i = 0; i < numRecords; i++) {
            Map<String, Object> eventMap = (Map<String, Object>) events.get(i);
            String name = (String)eventMap.get("name");
            assertTrue(expectedMap.containsKey(name));
            assertThat(expectedMap.get(name), equalTo(eventMap.get("age")));
            expectedMap.remove(name);
        }
        assertThat(expectedMap.size(), equalTo(0));
        assertThat(sqsSinkBatchEntry.getEventCount(), equalTo(numRecords));
        assertThat(sqsSinkBatchEntry.getGroupId(), equalTo(groupId));
        assertThat(sqsSinkBatchEntry.getDedupId(), equalTo(deDupId));
        assertThat(sqsSinkBatchEntry.getSize(), equalTo(expectedSize));
        assertThat(sqsSinkBatchEntry.getEventHandles().size(), equalTo(numRecords));
    }
    
    private List<Record<Event>> getRecordList(int numberOfRecords) {
        final List<Record<Event>> recordList = new ArrayList<>();
        List<HashMap> records = generateRecords(numberOfRecords);
        for (int i = 0; i < numberOfRecords; i++) {
            final Event event = JacksonLog.builder().withData(records.get(i)).build();
            recordList.add(new Record<>(event));
        }
        return recordList;
    }

    private static List<HashMap> generateRecords(int numberOfRecords) {

        List<HashMap> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {

            HashMap<String, String> eventData = new HashMap<>();

            eventData.put("name", "Person" + rows);
            eventData.put("age", Integer.toString(rows));
            recordList.add(eventData);

        }
        return recordList;
    }
}
