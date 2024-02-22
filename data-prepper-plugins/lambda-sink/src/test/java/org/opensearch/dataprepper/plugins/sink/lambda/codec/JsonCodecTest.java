/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.lambda.codec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonCodecTest {
    private ByteArrayOutputStream outputStream;

    private JsonCodec createObjectUnderTest() {
        String key = "event";
        return new JsonCodec(key);
    }

    @Test
    void test_happy_case_with_null_codec_key() throws IOException {
        JsonCodec jsonCodec = new JsonCodec(null);

        outputStream = new ByteArrayOutputStream();
        OutputCodecContext codecContext = new OutputCodecContext();
//        jsonCodec.start(outputStream, null, codecContext);
        jsonCodec.start(outputStream, null, codecContext);

        final List<Map<String, Object>> expectedData = generateRecords(1);
        final Event event = convertToEvent(expectedData.get(0));
        jsonCodec.writeEvent(event, outputStream);
        jsonCodec.complete(outputStream);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(outputStream.toByteArray());
        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.OBJECT));
        assertEquals(jsonNode.toString(),"{\"name\":\"Person0\",\"age\":0}");
    }


    @Test
    void test_happy_case_with_codec_key() throws IOException {
        String key = "events";
        final int numberOfRecords = 2;
        JsonCodec jsonCodec = new JsonCodec(key);

        outputStream = new ByteArrayOutputStream();
        OutputCodecContext codecContext = new OutputCodecContext();
        jsonCodec.start(outputStream, null, codecContext);

        final List<Map<String, Object>> expectedData = generateRecords(numberOfRecords);
        for (int index = 0; index < numberOfRecords; index++) {
            final Event event = convertToEvent(expectedData.get(index));
            jsonCodec.writeEvent(event, outputStream);
        }
        jsonCodec.complete(outputStream);

        String expectedString = "{\"events\":[{\"name\":\"Person0\",\"age\":0},{\"name\":\"Person1\",\"age\":1}]}";
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(outputStream.toByteArray());
        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.OBJECT));
        assertEquals(jsonNode.toString(),expectedString);
    }

    private static Event convertToEvent(Map<String, Object> data) {
        return JacksonLog.builder().withData(data).build();
    }

    private static List<Map<String, Object>> generateRecords(int numberOfRecords) {

        List<Map<String, Object>> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {

            Map<String, Object> eventData = new HashMap<>();

            eventData.put("name", "Person" + rows);
            eventData.put("age", rows);
            recordList.add(eventData);

        }

        return recordList;
    }


    private Object getValue(JsonNode jsonNode) {
        if(jsonNode.isTextual())
            return jsonNode.asText();

        if(jsonNode.isInt())
            return jsonNode.asInt();

        throw new RuntimeException("Test not setup correctly.");
    }
}