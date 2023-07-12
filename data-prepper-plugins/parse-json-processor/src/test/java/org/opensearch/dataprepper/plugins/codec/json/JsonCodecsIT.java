/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class JsonCodecsIT {

    private ObjectMapper objectMapper;
    private Consumer<Record<Event>> eventConsumer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();

        eventConsumer = mock(Consumer.class);
    }

    private JsonInputCodec createJsonInputCodecObjectUnderTest() {
        return new JsonInputCodec();
    }
    private JsonOutputCodec createJsonOutputCodecObjectUnderTest() {
        return new JsonOutputCodec(new JsonOutputCodecConfig());
    }
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void parse_with_InputStream_calls_Consumer_with_Event(final int numberOfObjects) throws IOException {
        final List<HashMap> initialRecords = generateRecords(numberOfObjects);
        ByteArrayInputStream inputStream = (ByteArrayInputStream) createInputStream(initialRecords);
        createJsonInputCodecObjectUnderTest().parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfObjects)).accept(recordArgumentCaptor.capture());

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();
        JsonOutputCodec jsonOutputCodec = createJsonOutputCodecObjectUnderTest();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        jsonOutputCodec.start(outputStream, null, null);

        assertThat(actualRecords.size(), equalTo(numberOfObjects));


        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            jsonOutputCodec.writeEvent(actualRecord.getData(),outputStream, null);
        }
        jsonOutputCodec.complete(outputStream);
        int index=0;
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(outputStream.toByteArray());
        for (JsonNode element : jsonNode) {
            Set<String> keys = initialRecords.get(index).keySet();
            Map<String , Object> actualMap = new HashMap<>();
            for(String key: keys){
                actualMap.put(key, element.get(key).asText());
            }
            assertThat(initialRecords.get(index), Matchers.equalTo(actualMap));
            index++;
        }
    }

    private InputStream createInputStream(final List<HashMap> jsonObjects) throws JsonProcessingException {
        final String keyName = UUID.randomUUID().toString();
        final Map<String, Object> jsonRoot = Collections.singletonMap(keyName, jsonObjects);
        return createInputStream(jsonRoot);
    }

    private InputStream createInputStream(final Map<String, ?> jsonRoot) throws JsonProcessingException {
        final byte[] jsonBytes = objectMapper.writeValueAsBytes(jsonRoot);

        return new ByteArrayInputStream(jsonBytes);
    }

    private static List<HashMap> generateRecords(int numberOfRecords) {

        List<HashMap> recordList = new ArrayList<>();

        for(int rows = 0; rows < numberOfRecords; rows++){

            HashMap<String, String> eventData = new HashMap<>();

            eventData.put("name", "Person"+rows);
            eventData.put("age", Integer.toString(rows));
            recordList.add((eventData));

        }
        return recordList;
    }
}
