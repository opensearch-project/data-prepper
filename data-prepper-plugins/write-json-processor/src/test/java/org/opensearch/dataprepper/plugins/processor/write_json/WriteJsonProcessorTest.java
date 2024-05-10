/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.write_json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import org.mockito.Mock;

import java.util.Map;
import java.util.List;
import java.util.UUID;

public class WriteJsonProcessorTest {
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Mock
    private WriteJsonProcessorConfig mockConfig;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private PluginMetrics pluginMetrics;

    private WriteJsonProcessor writeJsonProcessor;

    private String sourceKey;
    
    @BeforeEach
    void setup() {
        pluginMetrics = mock(PluginMetrics.class);
        pluginFactory = mock(PluginFactory.class);
        sourceKey = UUID.randomUUID().toString();
        mockConfig = mock(WriteJsonProcessorConfig.class);
        when(mockConfig.getSource()).thenReturn(sourceKey);
    }

    WriteJsonProcessor createObjectUnderTest() {
        return new WriteJsonProcessor(mockConfig, pluginMetrics, pluginFactory);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "targetKey"})
    public void testBasic(String target) throws Exception {
        String targetKey = (target.equals("")) ? sourceKey : target;
        when(mockConfig.getTarget()).thenReturn(targetKey);
        Map<String, Object> value1 = Map.of("stringKey", "testString", "intKey", 10);
        Map<String, Object> value = Map.of("mapKey", value1, "boolKey", true);
        String expectedString1 = "{\"mapKey\":{\"stringKey\":\"testString\",\"intKey\":10},\"boolKey\":true}";
        String expectedString2 = "{\"boolKey\":true,\"mapKey\":{\"stringKey\":\"testString\",\"intKey\":10}}";
        String expectedString3 = "{\"boolKey\":true,\"mapKey\":{\"intKey\":10,\"stringKey\":\"testString\"}}";
        String expectedString4 = "{\"mapKey\":{\"intKey\":10,\"stringKey\":\"testString\"},\"boolKey\":true}";


        Map<String, Object> data = Map.of(sourceKey, value);
        Record<Event> record = createRecord(data);

        writeJsonProcessor = createObjectUnderTest();
        final List<Record<Event>> outputRecords = (List<Record<Event>>) writeJsonProcessor.doExecute(List.of(record));
        assertThat(outputRecords.size(), equalTo(1));
        Event event = outputRecords.get(0).getData();
        String expectedResult = objectMapper.writeValueAsString(value);
        String targetInEvent = event.get(targetKey, String.class);
        assertThat(targetInEvent, equalTo(expectedResult));
        assertTrue(expectedString1.equals(targetInEvent)||expectedString2.equals(targetInEvent)||expectedString3.equals(targetInEvent)||expectedString4.equals(targetInEvent));
    
    }

    static Record<Event> createRecord(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
            .withData(data)
            .withEventType("event")
            .build());
    }

}
