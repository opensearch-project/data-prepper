/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


public class NewlineDelimitedOutputCodecTest {
    private ByteArrayOutputStream outputStream;

    private static NdjsonOutputConfig config;

    private static int numberOfRecords;
    private static final String REGEX = "\\r?\\n";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private NdjsonOutputCodec createObjectUnderTest() {
        config = new NdjsonOutputConfig();
        return new NdjsonOutputCodec(config);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3, 10, 100})
    void test_happy_case(final int numberOfRecords) throws IOException {
        NewlineDelimitedOutputCodecTest.numberOfRecords = numberOfRecords;
        NdjsonOutputCodec ndjsonOutputCodec = createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        OutputCodecContext codecContext = new OutputCodecContext();
        ndjsonOutputCodec.start(outputStream, null, codecContext);
        for (int index = 0; index < numberOfRecords; index++) {
            final Event event = (Event) getRecord(index).getData();
            ndjsonOutputCodec.writeEvent(event, outputStream);
        }
        ndjsonOutputCodec.complete(outputStream);
        String jsonString = outputStream.toString(StandardCharsets.UTF_8);
        int index = 0;
        List<HashMap> expectedRecords = generateRecords(numberOfRecords);
        String[] jsonObjects = jsonString.split(REGEX);
        for (String jsonObject : jsonObjects) {
            Object expectedMap = expectedRecords.get(index);
            Object actualMap = objectMapper.readValue(jsonObject, Map.class);
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
            recordList.add(eventData);
        }
        return recordList;
    }

    @Test
    void testGetExtension() {
        NdjsonOutputCodec ndjsonOutputCodec = createObjectUnderTest();

        assertThat("ndjson", equalTo(ndjsonOutputCodec.getExtension()));
    }

    @Test
    void testGetEstimatedSize() throws Exception {
        NewlineDelimitedOutputCodecTest.numberOfRecords = 1;
        NdjsonOutputCodec ndjsonOutputCodec = createObjectUnderTest();
        outputStream = new ByteArrayOutputStream();
        OutputCodecContext codecContext = new OutputCodecContext();
        ndjsonOutputCodec.start(outputStream, null, codecContext);

        Record<Event> record = getRecord(0);
        String expectedEventString = "{\"name\":\"Person0\",\"age\":0}\n";
        assertThat(ndjsonOutputCodec.getEstimatedSize(record.getData(), codecContext), equalTo((long)expectedEventString.length()));
    }
}
