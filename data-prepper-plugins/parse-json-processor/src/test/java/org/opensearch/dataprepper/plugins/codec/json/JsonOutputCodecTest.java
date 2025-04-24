/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class JsonOutputCodecTest {
    private ByteArrayOutputStream outputStream;

    private JsonOutputCodec createObjectUnderTest() {
        return new JsonOutputCodec(new JsonOutputCodecConfig());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_happy_case(final int numberOfRecords) throws IOException {
        JsonOutputCodec jsonOutputCodec = createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        OutputCodecContext codecContext = new OutputCodecContext();
        jsonOutputCodec.start(outputStream, null, codecContext);

        final List<Map<String, Object>> expectedData = generateRecords(numberOfRecords);
        for (int index = 0; index < numberOfRecords; index++) {
            final Event event = convertToEvent(expectedData.get(index));
            jsonOutputCodec.writeEvent(event, outputStream);
        }
        jsonOutputCodec.complete(outputStream);

        int index = 0;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(outputStream.toByteArray());
        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.OBJECT));
        Map.Entry<String, JsonNode> nextField = jsonNode.fields().next();
        assertThat(nextField, notNullValue());
        assertThat(nextField.getKey(), equalTo(JsonOutputCodecConfig.DEFAULT_KEY_NAME));
        jsonNode = nextField.getValue();
        assertThat(jsonNode, notNullValue());
        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.ARRAY));
        for (JsonNode actualElement : jsonNode) {
            Map<String, Object> expectedMap = expectedData.get(index);
            Set<String> keys = expectedMap.keySet();
            Map<String, Object> actualMap = new HashMap<>();
            for (String key : keys) {
                actualMap.put(key, getValue(actualElement.get(key)));
            }
            assertThat(actualMap, equalTo(expectedMap));
            index++;
        }

        assertThat(index, equalTo(numberOfRecords));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void writeEvent_with_include_keys(final int numberOfRecords) throws IOException {
        JsonOutputCodec jsonOutputCodec = createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        OutputCodecContext codecContext = new OutputCodecContext(null, List.of("name"), null);
        jsonOutputCodec.start(outputStream, null, codecContext);

        final List<Map<String, Object>> expectedData = generateRecords(numberOfRecords);
        for (int index = 0; index < numberOfRecords; index++) {
            final Event event = convertToEvent(expectedData.get(index));
            jsonOutputCodec.writeEvent(event, outputStream);
        }
        jsonOutputCodec.complete(outputStream);

        int index = 0;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(outputStream.toByteArray());
        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.OBJECT));
        Map.Entry<String, JsonNode> nextField = jsonNode.fields().next();
        assertThat(nextField, notNullValue());
        assertThat(nextField.getKey(), equalTo(JsonOutputCodecConfig.DEFAULT_KEY_NAME));
        jsonNode = nextField.getValue();
        assertThat(jsonNode, notNullValue());
        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.ARRAY));
        for (JsonNode actualElement : jsonNode) {
            Map<String, Object> expectedMap = expectedData.get(index);
            assertThat(actualElement.has("age"), equalTo(false));
            assertThat(actualElement.has("name"), equalTo(true));
            assertThat(actualElement.get("name").getNodeType(), equalTo(JsonNodeType.STRING));
            assertThat(actualElement.get("name").asText(), equalTo(expectedMap.get("name")));
            index++;
        }

        assertThat(index, equalTo(numberOfRecords));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void writeEvent_with_exclude_keys(final int numberOfRecords) throws IOException {
        JsonOutputCodec jsonOutputCodec = createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        OutputCodecContext codecContext = new OutputCodecContext(null, null, List.of("age"));
        jsonOutputCodec.start(outputStream, null, codecContext);

        final List<Map<String, Object>> expectedData = generateRecords(numberOfRecords);
        for (int index = 0; index < numberOfRecords; index++) {
            final Event event = convertToEvent(expectedData.get(index));
            jsonOutputCodec.writeEvent(event, outputStream);
        }
        jsonOutputCodec.complete(outputStream);

        int index = 0;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(outputStream.toByteArray());
        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.OBJECT));
        Map.Entry<String, JsonNode> nextField = jsonNode.fields().next();
        assertThat(nextField, notNullValue());
        assertThat(nextField.getKey(), equalTo(JsonOutputCodecConfig.DEFAULT_KEY_NAME));
        jsonNode = nextField.getValue();
        assertThat(jsonNode, notNullValue());
        assertThat(jsonNode.getNodeType(), equalTo(JsonNodeType.ARRAY));
        for (JsonNode actualElement : jsonNode) {
            Map<String, Object> expectedMap = expectedData.get(index);
            assertThat(actualElement.has("age"), equalTo(false));
            assertThat(actualElement.has("name"), equalTo(true));
            assertThat(actualElement.get("name").getNodeType(), equalTo(JsonNodeType.STRING));
            assertThat(actualElement.get("name").asText(), equalTo(expectedMap.get("name")));
            index++;
        }

        assertThat(index, equalTo(numberOfRecords));
    }

    @Test
    void testGetEstimatedSize() throws Exception {
        int numberOfRecords = 1;
        JsonOutputCodec jsonOutputCodec = createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        OutputCodecContext codecContext = new OutputCodecContext();
        final List<Map<String, Object>> expectedData = generateRecords(numberOfRecords);
        final Event event = convertToEvent(expectedData.get(0));
        jsonOutputCodec.start(outputStream, null, codecContext);
        String expectedEventString = "{\"events\":[{\"name\":\"Person0\",\"age\":0}]";
        assertThat(jsonOutputCodec.getEstimatedSize(event, codecContext), greaterThanOrEqualTo((long)(expectedEventString.length())));
    }

    @Test
    void testGetExtension() {
        JsonOutputCodec jsonOutputCodec = createObjectUnderTest();

        assertThat("json", equalTo(jsonOutputCodec.getExtension()));
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
