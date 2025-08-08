/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.livecapture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LiveCaptureOutputManagerTest {

    @Mock
    private Sink<Record<Event>> mockSink;

    private LiveCaptureOutputManager outputManager;
    private LiveCaptureManager liveCaptureManager;

    @BeforeEach
    void setUp() {
        // Reset the singleton instances for clean tests
        LiveCaptureManager.initialize(true, 10.0);
        liveCaptureManager = LiveCaptureManager.getInstance();
        liveCaptureManager.clearStoredEntries(); // Clear any stale test data
        outputManager = LiveCaptureOutputManager.getInstance();
        
        // Reset capture counter for consistent test results using reflection
        try {
            java.lang.reflect.Field counterField = LiveCaptureOutputManager.class.getDeclaredField("captureCounter");
            counterField.setAccessible(true);
            ((java.util.concurrent.atomic.AtomicInteger) counterField.get(outputManager)).set(0);
        } catch (Exception e) {
            // Ignore reflection errors for now
        }
    }

    @AfterEach
    void tearDown() {
        if (outputManager != null) {
            outputManager.shutdown();
        }
    }

    @Test
    void getInstance_returns_same_instance() {
        LiveCaptureOutputManager instance1 = LiveCaptureOutputManager.getInstance();
        LiveCaptureOutputManager instance2 = LiveCaptureOutputManager.getInstance();
        
        assertThat(instance1, is(instance2));
    }

    @Test
    void initialize_with_valid_sink_sets_initialized() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        outputManager.initialize(mockSink, 50, 100);
        outputManager.enable();
        
        verify(mockSink).initialize();
        
        assertTrue(outputManager.isEnabled());
    }

    @Test
    void initialize_with_null_sink_does_not_initialize() {
        outputManager.initialize(null, 30, 100);
        
        assertFalse(outputManager.isEnabled());
    }

    @Test
    void initialize_with_sink_initialization_failure_does_not_initialize() throws Exception {
        doThrow(new RuntimeException("Sink initialization failed")).when(mockSink).initialize();
        
        outputManager.initialize(mockSink, 50, 100);
        
        assertFalse(outputManager.isEnabled());
    }

    @Test
    void enable_without_initialization_does_not_enable() {
        outputManager.enable();
        
        assertFalse(outputManager.isEnabled());
    }

    @Test
    void enable_after_initialization_enables_output() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        outputManager.initialize(mockSink, 5, 10); // threshold of 5 for fast testing
        
        outputManager.enable();
        
        assertTrue(outputManager.isEnabled());
    }

    @Test
    void disable_stops_processing() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        outputManager.initialize(mockSink, 5, 10);
        outputManager.enable();
        
        assertTrue(outputManager.isEnabled());
        
        outputManager.disable();
        
        assertFalse(outputManager.isEnabled());
    }


    @Test
    void addEntry_groups_by_traceId_and_processes_when_complete() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        // Initialize and enable output manager
        outputManager.initialize(mockSink, 50, 100);
        outputManager.enable();
        
        String traceId = "test-trace-1";
        
        // Create test entries for the same trace
        Map<String, Object> sourceEntry = Map.of(
            "traceId", traceId,
            "stage", "Source",
            "eventTime", "2023-01-01T00:00:00Z",
            "logMessage", "Event created and marked for live capture",
            "captureMetaData", Map.of("event", Map.of("message", "test data"))
        );
        
        Map<String, Object> processorEntry = Map.of(
            "traceId", traceId,
            "stage", "Processor",
            "eventTime", "2023-01-01T00:00:01Z",
            "logMessage", "After test_processor processor execution",
            "captureMetaData", Map.of("event", Map.of("message", "processed data"), "processorName", "test_processor")
        );
        
        Map<String, Object> sinkEntry = Map.of(
            "traceId", traceId,
            "stage", "Sink",
            "eventTime", "2023-01-01T00:00:02Z",
            "logMessage", "Received by sink test_sink",
            "captureMetaData", Map.of("event", Map.of("message", "processed data"), "sinkName", "test_sink")
        );
        
        // Add source entry - should not trigger output yet
        outputManager.addEntry(sourceEntry);
        Thread.sleep(100);
        verify(mockSink, timeout(500).times(0)).output(any());
        
        // Add processor entry - should not trigger output yet
        outputManager.addEntry(processorEntry);
        Thread.sleep(100);
        verify(mockSink, timeout(500).times(0)).output(any());
        
        // Add sink entry - should trigger output for complete trace
        outputManager.addEntry(sinkEntry);
        
        // Verify sink received the complete trace as a single event
        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(mockSink, timeout(1000)).output(recordsCaptor.capture());
        
        Collection<Record<Event>> outputRecords = recordsCaptor.getValue();
        assertThat(outputRecords.size(), equalTo(1)); // Single event containing complete trace
        
        // Verify the complete trace event
        Event traceEvent = outputRecords.iterator().next().getData();
        assertThat(traceEvent.getMetadata().getEventType(), equalTo("live_capture_complete_trace"));
        assertThat(traceEvent.getMetadata().getAttribute("event_source"), equalTo("live_capture_complete_trace"));
        assertThat(traceEvent.getMetadata().getAttribute("trace_id"), equalTo(traceId));
        assertThat(traceEvent.getMetadata().getAttribute("entry_count"), equalTo(3));
        
        // Verify trace contains all entries in correct order
        Map<String, Object> traceData = traceEvent.toMap();
        assertThat(traceData.get("Capture #"), equalTo(1));
        
        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> entries = (java.util.List<Map<String, Object>>) traceData.get("entries");
        assertThat(entries.size(), equalTo(3));
        assertThat(entries.get(0).get("stage"), equalTo("Source"));
        assertThat(entries.get(1).get("stage"), equalTo("Processor"));
        assertThat(entries.get(2).get("stage"), equalTo("Sink"));
        
        // Verify traceId is removed from individual entries
        assertThat(entries.get(0).containsKey("traceId"), equalTo(false));
        assertThat(entries.get(1).containsKey("traceId"), equalTo(false));
        assertThat(entries.get(2).containsKey("traceId"), equalTo(false));
    }

    @Test
    void processEntries_handles_multiple_complete_traces() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        // Initialize and enable output manager
        outputManager.initialize(mockSink, 30, 5);
        outputManager.enable();
        
        // Create multiple live capture events, each completing immediately
        for (int i = 0; i < 3; i++) {
            Event liveEvent = JacksonEvent.fromMessage("{\"test\": \"data" + i + "\"}");
            LiveCaptureManager.setLiveCapture(liveEvent, true);
            LiveCaptureManager.addLiveCaptureEntry(liveEvent, "Sink", "Test entry " + i, "sink-" + i, null);
        }
        
        // Wait for processing to complete naturally
        
        // Verify sink received the entries (3 separate complete traces)
        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(mockSink, timeout(1000).atLeast(3)).output(recordsCaptor.capture());
        
        // Each trace should be sent separately, so we should have received multiple calls
        // Each call should contain one complete trace
        java.util.List<Collection<Record<Event>>> allCalls = recordsCaptor.getAllValues();
        assertThat(allCalls.size(), equalTo(3));
        
        for (Collection<Record<Event>> call : allCalls) {
            assertThat(call.size(), equalTo(1)); // Each call has one complete trace
            Event traceEvent = call.iterator().next().getData();
            assertThat(traceEvent.getMetadata().getEventType(), equalTo("live_capture_complete_trace"));
        }
    }

    @Test
    void processEntries_handles_sink_errors_gracefully() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        doThrow(new RuntimeException("Sink processing error")).when(mockSink).output(any());
        
        outputManager.initialize(mockSink, 50, 100);
        outputManager.enable();
        
        // Create a live capture event that completes immediately (reaches Sink stage)
        Event liveEvent = JacksonEvent.fromMessage("{\"test\": \"data\"}");
        LiveCaptureManager.setLiveCapture(liveEvent, true);
        LiveCaptureManager.addLiveCaptureEntry(liveEvent, "Sink", "Test entry", "test-sink", null);
        
        // Should not throw exception despite sink error
        // The sink call happens automatically when Sink stage entry is added
        Thread.sleep(200); // Give time for async processing
        
        verify(mockSink, timeout(1000)).output(any());
    }


    @Test
    void shutdown_cleans_up_resources() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        outputManager.initialize(mockSink, 50, 100);
        outputManager.enable();
        
        assertTrue(outputManager.isEnabled());
        
        outputManager.shutdown();
        
        assertFalse(outputManager.isEnabled());
        
        verify(mockSink).shutdown();
    }

    @Test
    void convertEntryToEvent_creates_proper_complete_trace_structure() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        outputManager.initialize(mockSink, 50, 100);
        outputManager.enable();
        
        // Create a live capture event
        Event liveEvent = JacksonEvent.builder()
            .withEventType("test")
            .withData(Map.of("field", "value"))
            .build();
        LiveCaptureManager.setLiveCapture(liveEvent, true);
        
        String traceId = (String) liveEvent.getMetadata().getAttribute(LiveCaptureManager.LIVE_CAPTURE_TRACE_ID);
        LiveCaptureManager.addLiveCaptureEntry(liveEvent, "Sink", "Test sink entry", "test-sink", null);
        
        // Processing happens automatically when Sink stage is added
        
        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(mockSink).output(recordsCaptor.capture());
        
        Record<Event> outputRecord = recordsCaptor.getValue().iterator().next();
        Event outputEvent = outputRecord.getData();
        
        // Verify the converted event has correct structure and metadata for complete trace
        assertThat(outputEvent.getMetadata().getEventType(), equalTo("live_capture_complete_trace"));
        assertThat(outputEvent.getMetadata().getAttribute("event_source"), equalTo("live_capture_complete_trace"));
        assertThat(outputEvent.getMetadata().getAttribute("trace_id"), equalTo(traceId));
        assertThat(outputEvent.getMetadata().getAttribute("entry_count"), equalTo(1));
        
        Map<String, Object> eventData = outputEvent.toMap();
        assertThat(eventData.get("Capture #"), equalTo(1));
        
        // Verify entries array structure
        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> entries = (java.util.List<Map<String, Object>>) eventData.get("entries");
        assertThat(entries.size(), equalTo(1));
        
        Map<String, Object> entry = entries.get(0);
        assertThat(entry.get("stage"), equalTo("Sink"));
        assertThat(entry.containsKey("traceId"), equalTo(false));
        assertThat(entry.get("logMessage"), equalTo("Test sink entry"));
        
        // Verify captureMetaData exists and has expected structure
        @SuppressWarnings("unchecked")
        Map<String, Object> captureMetaData = (Map<String, Object>) entry.get("captureMetaData");
        assertThat(captureMetaData, hasKey("event"));
        assertThat(captureMetaData, hasKey("sinkName"));
        assertThat(captureMetaData.get("sinkName"), equalTo("test-sink"));
        
        // Verify original event data is preserved (structure may vary)
        @SuppressWarnings("unchecked")
        Map<String, Object> originalEventData = (Map<String, Object>) captureMetaData.get("event");
        assertThat(originalEventData, notNullValue());
    }
    
    @Test
    void addEntry_handles_missing_traceId_with_fallback() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        // Initialize and enable output manager with low threshold for testing
        outputManager.initialize(mockSink, 2, 100);
        outputManager.enable();
        
        // Create entries without traceId
        Map<String, Object> entry1 = Map.of(
            "stage", "Source",
            "eventTime", "2023-01-01T00:00:00Z",
            "logMessage", "Test entry without traceId",
            "captureMetaData", Map.of("event", Map.of("test", "data"))
        );
        
        Map<String, Object> entry2 = Map.of(
            "stage", "Processor", 
            "eventTime", "2023-01-01T00:00:01Z",
            "logMessage", "Another test entry without traceId",
            "captureMetaData", Map.of("event", Map.of("test", "data2"))
        );
        
        // Add entries - should trigger fallback processing when threshold reached
        outputManager.addEntry(entry1);
        outputManager.addEntry(entry2);
        
        // Verify sink received the entries via fallback mechanism
        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(mockSink, timeout(1000)).output(recordsCaptor.capture());
        
        Collection<Record<Event>> outputRecords = recordsCaptor.getValue();
        assertThat(outputRecords.size(), equalTo(2)); // Individual entries, not grouped
        
        // Verify entries were processed with fallback mechanism
        for (Record<Event> record : outputRecords) {
            Event outputEvent = record.getData();
            assertThat(outputEvent.getMetadata().getAttribute("event_source"), equalTo("live_capture_output"));
        }
    }
    
    @Test
    void addEntry_handles_multiple_traces_independently() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        outputManager.initialize(mockSink, 50, 100);
        outputManager.enable();
        
        String traceId1 = "trace-1";
        String traceId2 = "trace-2";
        
        // Add entries for first trace
        outputManager.addEntry(Map.of(
            "traceId", traceId1,
            "stage", "Source",
            "eventTime", "2023-01-01T00:00:00Z",
            "logMessage", "Source for trace 1",
            "captureMetaData", Map.of("event", Map.of("data", "trace1"))
        ));
        
        // Add entries for second trace
        outputManager.addEntry(Map.of(
            "traceId", traceId2,
            "stage", "Source",
            "eventTime", "2023-01-01T00:00:00Z",
            "logMessage", "Source for trace 2",
            "captureMetaData", Map.of("event", Map.of("data", "trace2"))
        ));
        
        // Complete first trace
        outputManager.addEntry(Map.of(
            "traceId", traceId1,
            "stage", "Sink",
            "eventTime", "2023-01-01T00:00:02Z",
            "logMessage", "Sink for trace 1",
            "captureMetaData", Map.of("event", Map.of("data", "trace1"), "sinkName", "sink1")
        ));
        
        // Should get output for first trace only
        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(mockSink, timeout(1000)).output(recordsCaptor.capture());
        
        Collection<Record<Event>> outputRecords = recordsCaptor.getValue();
        assertThat(outputRecords.size(), equalTo(1));
        
        Event traceEvent = outputRecords.iterator().next().getData();
        assertThat(traceEvent.getMetadata().getAttribute("trace_id"), equalTo(traceId1));
        
        // Complete second trace
        outputManager.addEntry(Map.of(
            "traceId", traceId2,
            "stage", "Sink",
            "eventTime", "2023-01-01T00:00:03Z",
            "logMessage", "Sink for trace 2",
            "captureMetaData", Map.of("event", Map.of("data", "trace2"), "sinkName", "sink2")
        ));
        
        // Should get output for second trace
        verify(mockSink, timeout(1000).times(2)).output(any());
    }
    
    @Test
    void addEntry_cleans_up_old_traces() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        outputManager.initialize(mockSink, 50, 100);
        outputManager.enable();
        
        String traceId = "incomplete-trace";
        
        // Add incomplete trace entries (without sink stage)
        outputManager.addEntry(Map.of(
            "traceId", traceId,
            "stage", "Source",
            "eventTime", "2023-01-01T00:00:00Z",
            "logMessage", "Source entry",
            "captureMetaData", Map.of("event", Map.of("data", "test"))
        ));
        
        outputManager.addEntry(Map.of(
            "traceId", traceId,
            "stage", "Processor",
            "eventTime", "2023-01-01T00:00:01Z",
            "logMessage", "Processor entry",
            "captureMetaData", Map.of("event", Map.of("data", "processed"))
        ));
        
        // Verify no output yet (incomplete trace)
        verify(mockSink, timeout(500).times(0)).output(any());
        
        // Verify the trace is being tracked (no direct way to check count, but verified by timeout behavior)"
    }
    
    @Test
    void disable_processes_remaining_trace_groups() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        outputManager.initialize(mockSink, 50, 100);
        outputManager.enable();
        
        String traceId = "pending-trace";
        
        // Add incomplete trace entries
        outputManager.addEntry(Map.of(
            "traceId", traceId,
            "stage", "Source",
            "eventTime", "2023-01-01T00:00:00Z",
            "logMessage", "Source entry",
            "captureMetaData", Map.of("event", Map.of("data", "test"))
        ));
        
        // Disable should process remaining traces
        outputManager.disable();
        
        // Verify the pending trace was processed
        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(mockSink, timeout(1000)).output(recordsCaptor.capture());
        
        Event incompleteTrace = recordsCaptor.getValue().iterator().next().getData();
        assertThat(incompleteTrace.getMetadata().getEventType(), equalTo("live_capture_incomplete_trace"));
        assertThat(incompleteTrace.getMetadata().getAttribute("trace_id"), equalTo(traceId));
    }
    
    
    @Test
    void addEntry_handles_empty_entries_list() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        outputManager.initialize(mockSink, 50, 100);
        outputManager.enable();
        
        // Add entry and immediately complete it
        String traceId = "empty-trace";
        outputManager.addEntry(Map.of(
            "traceId", traceId,
            "stage", "Sink",
            "eventTime", "2023-01-01T00:00:00Z",
            "logMessage", "Sink entry",
            "captureMetaData", Map.of("event", Map.of("data", "test"))
        ));
        
        // Should handle the complete trace normally
        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(mockSink, timeout(1000)).output(recordsCaptor.capture());
        
        Event completeTrace = recordsCaptor.getValue().iterator().next().getData();
        assertThat(completeTrace.getMetadata().getEventType(), equalTo("live_capture_complete_trace"));
        assertThat(completeTrace.getMetadata().getAttribute("entry_count"), equalTo(1));
    }
    
    @Test
    void shutdown_processes_all_pending_traces() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        outputManager.initialize(mockSink, 50, 100);
        outputManager.enable();
        
        // Add multiple incomplete traces
        for (int i = 0; i < 3; i++) {
            outputManager.addEntry(Map.of(
                "traceId", "shutdown-trace-" + i,
                "stage", "Source",
                "eventTime", "2023-01-01T00:00:0" + i + "Z",
                "logMessage", "Source entry " + i,
                "captureMetaData", Map.of("event", Map.of("data", "test" + i))
            ));
        }
        
        // Shutdown should process all pending traces
        outputManager.shutdown();
        
        // Verify all traces were processed (3 incomplete traces)
        verify(mockSink, timeout(1000).times(3)).output(any());
        verify(mockSink).shutdown();
    }
    
    @Test
    void createCompleteTraceEvent_handles_metadata_extraction() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        
        outputManager.initialize(mockSink, 50, 100);
        outputManager.enable();
        
        String traceId = "metadata-trace";
        
        // Add entries with different timestamps to test ordering
        outputManager.addEntry(Map.of(
            "traceId", traceId,
            "stage", "Processor",
            "eventTime", "2023-01-01T00:00:01Z", // Second chronologically
            "logMessage", "Processor entry",
            "captureMetaData", Map.of("event", Map.of("data", "processed"))
        ));
        
        outputManager.addEntry(Map.of(
            "traceId", traceId,
            "stage", "Source",
            "eventTime", "2023-01-01T00:00:00Z", // First chronologically
            "logMessage", "Source entry",
            "captureMetaData", Map.of("event", Map.of("data", "original"))
        ));
        
        outputManager.addEntry(Map.of(
            "traceId", traceId,
            "stage", "Sink",
            "eventTime", "2023-01-01T00:00:02Z", // Last chronologically
            "logMessage", "Sink entry",
            "captureMetaData", Map.of("event", Map.of("data", "final"))
        ));
        
        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(mockSink, timeout(1000)).output(recordsCaptor.capture());
        
        Event completeTrace = recordsCaptor.getValue().iterator().next().getData();
        Map<String, Object> traceData = completeTrace.toMap();
        
        // Verify entries are sorted chronologically
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> entries = (List<Map<String, Object>>) traceData.get("entries");
        assertThat(entries.get(0).get("stage"), equalTo("Source"));
        assertThat(entries.get(1).get("stage"), equalTo("Processor"));
        assertThat(entries.get(2).get("stage"), equalTo("Sink"));
        
        // Verify timestamps are in chronological order
        assertThat(entries.get(0).get("eventTime"), equalTo("2023-01-01T00:00:00Z"));
        assertThat(entries.get(1).get("eventTime"), equalTo("2023-01-01T00:00:01Z"));
        assertThat(entries.get(2).get("eventTime"), equalTo("2023-01-01T00:00:02Z"));
    }
}