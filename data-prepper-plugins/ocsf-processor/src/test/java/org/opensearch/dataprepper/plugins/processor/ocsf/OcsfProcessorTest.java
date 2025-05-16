package org.opensearch.dataprepper.plugins.processor.ocsf;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class OcsfProcessorTest {
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter successCounter;
    @Mock
    private Counter failureCounter;
    @Mock
    private Timer processingTimer;

    private OcsfProcessor ocsfProcessor;

    @BeforeEach
    void setUp() {
        lenient().when(pluginMetrics.counter(anyString())).thenReturn(successCounter);
        lenient().when(pluginMetrics.timer(anyString())).thenReturn(processingTimer);
        when(pluginMetrics.counter("recordsFailed")).thenReturn(failureCounter);

        OcsfProcessorConfig config = new OcsfProcessorConfig() {
            @Override
            public String getSchemaType() {
                return "office365";
            }
        };

        ocsfProcessor = new OcsfProcessor(pluginMetrics, config) {
            @Override
            protected String getSchemaPath(String schemaType) {
                return getClass().getClassLoader().getResource("schemas/test-mapping.json").getPath();
            }
        };
    }

    @Test
    void testProcessAuthenticationEvent() {
        Map<String, Object> sourceData = new HashMap<>();
        sourceData.put("Operation", "UserLoggedIn");
        sourceData.put("Workload", "AzureActiveDirectory");
        sourceData.put("ResultStatus", "Success");
        sourceData.put("UserType", 0);

        Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(sourceData)
                .build();

        Collection<Record<Event>> processedRecords = ocsfProcessor.doExecute(
                Collections.singletonList(new Record<>(event)));

        Event processedEvent = processedRecords.iterator().next().getData();
        assertEquals("Authentication", processedEvent.get("class_name", String.class));
        assertEquals("Logon", processedEvent.get("activity_name", String.class));
        assertEquals(1, processedEvent.get("status_id", Integer.class));
        assertEquals(0, ((Map<String, Object>) ((Map<String, Object>) processedEvent.get("actor", Map.class)).get("user")).get("type_id"));
        verify(successCounter, times(1)).increment();
    }

    @Test
    void testProcessExchangeEvent() {
        Map<String, Object> sourceData = new HashMap<>();
        sourceData.put("Operation", "Create");
        sourceData.put("Workload", "Exchange");

        Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(sourceData)
                .build();

        Collection<Record<Event>> processedRecords = ocsfProcessor.doExecute(
                Collections.singletonList(new Record<>(event)));

        Event processedEvent = processedRecords.iterator().next().getData();
        assertEquals("Email Activity", processedEvent.get("class_name", String.class));
        assertEquals("Send", processedEvent.get("activity_name", String.class));
        verify(successCounter, times(1)).increment();
    }

    @Test
    void testProcessSharePointEvent() {
        Map<String, Object> sourceData = new HashMap<>();
        sourceData.put("Operation", "PageViewed");
        sourceData.put("Workload", "SharePoint");
        sourceData.put("ObjectId", "https://example.sharepoint.com/page");
        sourceData.put("Site", "site-id");

        Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(sourceData)
                .build();

        Collection<Record<Event>> processedRecords = ocsfProcessor.doExecute(
                Collections.singletonList(new Record<>(event)));

        Event processedEvent = processedRecords.iterator().next().getData();
        assertEquals("Web Resources Activity", processedEvent.get("class_name", String.class));
        assertEquals("Read", processedEvent.get("activity_name", String.class));
        assertEquals("site-id", ((Map<String, Object>) processedEvent.get("web_resources[]", Map.class)).get("uid"));
        verify(successCounter, times(1)).increment();
    }

    @Test
    void testHandleInvalidEvent() {
        Map<String, Object> sourceData = new HashMap<>();
        sourceData.put("UserType", "a");  // Invalid integer

        Event event = JacksonEvent.builder()
                .withEventType("event")
                .withData(sourceData)
                .build();

        Collection<Record<Event>> processedRecords = ocsfProcessor.doExecute(
                Collections.singletonList(new Record<>(event)));

        verify(failureCounter, times(1)).increment();
        verify(successCounter, never()).increment();
        assertEquals("a", processedRecords.iterator().next().getData()
                .get("UserType", String.class));
    }
}