/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.detect_format;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.event.EventBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class DetectFormatProcessorTest {
    private static final String TEST_SOURCE_KEY = "testKey";
    private static final String TARGET_KEY = "format";
    private static final String TARGET_METADATA_KEY = "format";
    
    private DetectFormatProcessor detectFormatProcessor;
    private DetectFormatProcessorConfig detectFormatProcessorConfig;
    private PluginMetrics pluginMetrics;
    private ExpressionEvaluator expressionEvaluator;
    private String condition;

    @BeforeEach
    void setup() {
        pluginMetrics = mock(PluginMetrics.class);
        expressionEvaluator = mock(ExpressionEvaluator.class);
        detectFormatProcessorConfig = mock(DetectFormatProcessorConfig.class);
        when(detectFormatProcessorConfig.getSource()).thenReturn(TEST_SOURCE_KEY);
        when(detectFormatProcessorConfig.getDetectWhen()).thenReturn(condition);
        when(detectFormatProcessorConfig.getTargetKey()).thenReturn(TARGET_KEY);
        when(detectFormatProcessorConfig.getTargetMetadataKey()).thenReturn(TARGET_METADATA_KEY);
        when(detectFormatProcessorConfig.getKVSeparatorList()).thenReturn(List.of(",", " ", "&"));
        when(detectFormatProcessorConfig.getKVDelimiter()).thenReturn("=");
        when(expressionEvaluator.evaluateConditional(anyString(), any(Event.class))).thenReturn(true);
    }

    DetectFormatProcessor createObjectUnderTest() {
        return new DetectFormatProcessor( detectFormatProcessorConfig, pluginMetrics, expressionEvaluator);
    }

    @Test
    public void testDetectFormatProcessorForJson() {
        Map<String, Object> data = Map.of(TEST_SOURCE_KEY, "{\"key1\":\"value1\", \"key2\" : 2}");
        detectFormatProcessor = createObjectUnderTest();
        List<Record<Event>> records = (List<Record<Event>>)detectFormatProcessor.doExecute((Collection<Record<Event>>)List.of(getTestRecord(data)));
        assertThat(records.size(), equalTo(1));
        Event event = records.get(0).getData();
        assertThat(event.get(TARGET_KEY, String.class), equalTo("json"));
        assertThat(event.getMetadata().getAttribute(TARGET_METADATA_KEY), equalTo("json"));
    }

    @Test
    public void testDetectFormatProcessorForNotJson() {
        Map<String, Object> data1 = Map.of(TEST_SOURCE_KEY, "{\"key1\":\"value1\", \"key2\" : 2");
        Map<String, Object> data2 = Map.of(TEST_SOURCE_KEY, "\"key1\":\"value1\", \"key2\" : 2");
        detectFormatProcessor = createObjectUnderTest();
        List<Record<Event>> records = (List<Record<Event>>)detectFormatProcessor.doExecute((Collection<Record<Event>>)List.of(getTestRecord(data1), getTestRecord(data2)));
        assertThat(records.size(), equalTo(2));
        for (int i = 0; i < 2; i++) {
            Event event = records.get(i).getData();
            assertNull(event.get(TARGET_KEY, String.class));
            assertNull(event.getMetadata().getAttribute(TARGET_METADATA_KEY));
        }
    }

    @Test
    public void testDetectFormatProcessorForXML() {
        Map<String, Object> data1 = Map.of(TEST_SOURCE_KEY, "<?xml version=\"1.0\" encoding=\"UTF-8\"><test>test xml</test>");
        Map<String, Object> data2 = Map.of(TEST_SOURCE_KEY, "<test>test xml</test>");
        detectFormatProcessor = createObjectUnderTest();
        List<Record<Event>> records = (List<Record<Event>>)detectFormatProcessor.doExecute((Collection<Record<Event>>)List.of(getTestRecord(data1), getTestRecord(data2)));
        assertThat(records.size(), equalTo(2));
        for (int i=0; i < 2; i++) {
            Event event = records.get(i).getData();
            assertThat(event.get(TARGET_KEY, String.class), equalTo("xml"));
            assertThat(event.getMetadata().getAttribute(TARGET_METADATA_KEY), equalTo("xml"));
        }
    }

    @Test
    public void testDetectFormatProcessorForNotXML() {
        Map<String, Object> data1 = Map.of(TEST_SOURCE_KEY, "xml version=\"1.0\" encoding=\"UTF-8\"><test>test xml</test>");
        Map<String, Object> data2 = Map.of(TEST_SOURCE_KEY, "<version=\"1.0\" encoding=\"UTF-8\"><test>test xml</test");
        Map<String, Object> data3 = Map.of(TEST_SOURCE_KEY, "<xml version=\"1.0\" encoding=\"UTF-8\"><test>test xml</test");
        detectFormatProcessor = createObjectUnderTest();
        List<Record<Event>> records = (List<Record<Event>>)detectFormatProcessor.doExecute((Collection<Record<Event>>)List.of(getTestRecord(data1), getTestRecord(data2), getTestRecord(data3)));
        assertThat(records.size(), equalTo(3));
        for (int i=0; i < 3; i++) {
            Event event = records.get(i).getData();
            assertThat(event.get(TARGET_KEY, String.class), not(equalTo("xml")));
            assertThat(event.getMetadata().getAttribute(TARGET_METADATA_KEY), not(equalTo("xml")));
        }
    }

    @Test
    public void testDetectFormatProcessorForCSV() {
        Map<String, Object> data = Map.of(TEST_SOURCE_KEY, "key1,key2,key3,key4\n1,2,3,4\na,b,c,d");
        detectFormatProcessor = createObjectUnderTest();
        List<Record<Event>> records = (List<Record<Event>>)detectFormatProcessor.doExecute((Collection<Record<Event>>)List.of(getTestRecord(data)));
        assertThat(records.size(), equalTo(1));
        Event event = records.get(0).getData();
        assertThat(event.get(TARGET_KEY, String.class), equalTo("csv"));
        assertThat(event.getMetadata().getAttribute(TARGET_METADATA_KEY), equalTo("csv"));
    }

    @Test
    public void testDetectFormatProcessorForNotCSV() {
        Map<String, Object> data1 = Map.of(TEST_SOURCE_KEY, "k1,k2,k3,k4\nabc\n2,3\n5\n");
        Map<String, Object> data2 = Map.of(TEST_SOURCE_KEY, "<xml version=\"1.0\" encoding=\"UTF-8\"><test>test xml</test");
        Map<String, Object> data3 = Map.of(TEST_SOURCE_KEY, "test string,");
        detectFormatProcessor = createObjectUnderTest();
        List<Record<Event>> records = (List<Record<Event>>)detectFormatProcessor.doExecute((Collection<Record<Event>>)List.of(getTestRecord(data1), getTestRecord(data2), getTestRecord(data3)));
        assertThat(records.size(), equalTo(3));
        for (int i=0; i < 3; i++) {
            Event event = records.get(i).getData();
            assertNull(event.get(TARGET_KEY, String.class));
            assertNull(event.getMetadata().getAttribute(TARGET_METADATA_KEY));
        }
    
    }

    @Test
    public void testDetectFormatProcessorForKeyValue() {
        Map<String, Object> data = Map.of(TEST_SOURCE_KEY, "k1=v1,k2=2,k3=\"v3\",k4='v4'");
        detectFormatProcessor = createObjectUnderTest();
        List<Record<Event>> records = (List<Record<Event>>)detectFormatProcessor.doExecute((Collection<Record<Event>>)List.of(getTestRecord(data)));
        assertThat(records.size(), equalTo(1));
        Event event = records.get(0).getData();
        assertThat(event.get(TARGET_KEY, String.class), equalTo("keyvalue"));
        assertThat(event.getMetadata().getAttribute(TARGET_METADATA_KEY), equalTo("keyvalue"));
    }

    @Test
    public void testDetectFormatProcessorForKeyValueWithNonDefaults() {
        when(detectFormatProcessorConfig.getKVSeparatorList()).thenReturn(List.of("-", ";", "^"));
        when(detectFormatProcessorConfig.getKVDelimiter()).thenReturn(":");
        Map<String, Object> data = Map.of(TEST_SOURCE_KEY, "k1:v1^k2:2;k3:\"v3\"-k4:'v4'");
        detectFormatProcessor = createObjectUnderTest();
        List<Record<Event>> records = (List<Record<Event>>)detectFormatProcessor.doExecute((Collection<Record<Event>>)List.of(getTestRecord(data)));
        assertThat(records.size(), equalTo(1));
        Event event = records.get(0).getData();
        assertThat(event.get(TARGET_KEY, String.class), equalTo("keyvalue"));
        assertThat(event.getMetadata().getAttribute(TARGET_METADATA_KEY), equalTo("keyvalue"));
    }


    @Test
    public void testDetectFormatProcessorForNotKeyValue() {
        Map<String, Object> data1 = Map.of(TEST_SOURCE_KEY, "test string");
        Map<String, Object> data2 = Map.of(TEST_SOURCE_KEY, "k====v");
        Map<String, Object> data3 = Map.of(TEST_SOURCE_KEY, "a,b,c,d");
        detectFormatProcessor = createObjectUnderTest();
        List<Record<Event>> records = (List<Record<Event>>)detectFormatProcessor.doExecute((Collection<Record<Event>>)List.of(getTestRecord(data1), getTestRecord(data2), getTestRecord(data3)));
        assertThat(records.size(), equalTo(3));
        for (int i=0; i < 3; i++) {
            Event event = records.get(i).getData();
            assertNull(event.get(TARGET_KEY, String.class));
            assertNull(event.getMetadata().getAttribute(TARGET_METADATA_KEY));
        }
    
    }


    @Test
    public void testDetectFormatProcessorForUnknownFormat() {
        Map<String, Object> data = Map.of(TEST_SOURCE_KEY, "192.168.2.20 - - [28/Jul/2006:10:27:10 -0300] \"GET /cgi-bin/try/ HTTP/1.0\" 200 3395");
        detectFormatProcessor = createObjectUnderTest();
        List<Record<Event>> records = (List<Record<Event>>)detectFormatProcessor.doExecute((Collection<Record<Event>>)List.of(getTestRecord(data)));
        assertThat(records.size(), equalTo(1));
        Event event = records.get(0).getData();
        assertNull(event.get(TARGET_KEY, String.class));
        assertNull(event.getMetadata().getAttribute(TARGET_METADATA_KEY));
    }


    private Record<Event> getTestRecord(final Map<String, Object> data) {
        return new Record<>(TestEventFactory
               .getTestEventFactory()
               .eventBuilder(EventBuilder.class)
               .withData(data)
               .withEventType("event")
               .build());
    }
}

