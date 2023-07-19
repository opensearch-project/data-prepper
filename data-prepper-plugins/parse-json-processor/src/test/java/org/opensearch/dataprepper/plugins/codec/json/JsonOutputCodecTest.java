/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class JsonOutputCodecTest {

    private static int numberOfRecords;
    private ByteArrayOutputStream outputStream;

    private static Record getRecord(int index) {
        List<HashMap> recordList = generateRecords(numberOfRecords);
        final Event event = JacksonLog.builder().withData(recordList.get(index)).build();
        return new Record<>(event);
    }

    private static List<HashMap> generateRecords(int numberOfRecords) {

        List<HashMap> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {

            HashMap<String, String> eventData = new HashMap<>();

            eventData.put("name", "Person" + rows);
            eventData.put("age", Integer.toString(rows));
            recordList.add((eventData));

        }
        return recordList;
    }

    private JsonOutputCodec createObjectUnderTest() {
        return new JsonOutputCodec(new JsonOutputCodecConfig());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_happy_case(final int numberOfRecords) throws IOException {
        JsonOutputCodecTest.numberOfRecords = numberOfRecords;
        JsonOutputCodec jsonOutputCodec = createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        jsonOutputCodec.start(outputStream, null, null);
        for (int index = 0; index < numberOfRecords; index++) {
            final Event event = (Event) getRecord(index).getData();
            jsonOutputCodec.writeEvent(event, outputStream, null);
        }
        jsonOutputCodec.complete(outputStream);
        List<HashMap> expectedRecords = generateRecords(numberOfRecords);
        int index = 0;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(outputStream.toByteArray());
        for (JsonNode element : jsonNode) {
            Set<String> keys = expectedRecords.get(index).keySet();
            Map<String, Object> actualMap = new HashMap<>();
            for (String key : keys) {
                actualMap.put(key, element.get(key).asText());
            }
            assertThat(expectedRecords.get(index), Matchers.equalTo(actualMap));
            index++;

        }
    }
    @Test
    void testGetExtension() {
        JsonOutputCodec jsonOutputCodec = createObjectUnderTest();

        assertThat("json", equalTo(jsonOutputCodec.getExtension()));
    }
}