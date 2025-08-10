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
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LiveCaptureOutputManagerTest {

    @Mock
    private Sink<Record<Event>> mockSink;

    private LiveCaptureOutputManager outputManager;

    @BeforeEach
    void setUp() {
        // Reset the singleton instances for clean tests
        LiveCaptureManager.initialize(true, 10.0);
        outputManager = LiveCaptureOutputManager.getInstance();

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
        outputManager.initialize(mockSink);

        assertTrue(outputManager.isEnabled() == false); // Not enabled by default
        verify(mockSink).initialize();
    }

    @Test
    void initialize_with_null_sink_does_not_initialize() {
        outputManager.initialize(null);

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

        outputManager.initialize(mockSink);
        outputManager.enable();

        assertTrue(outputManager.isEnabled());
    }

    @Test
    void disable_stops_processing() throws Exception {
        when(mockSink.isReady()).thenReturn(true);

        outputManager.initialize(mockSink);
        outputManager.enable();
        assertTrue(outputManager.isEnabled());

        outputManager.disable();
        assertFalse(outputManager.isEnabled());
    }

    @Test
    void processEventLiveCapture_handles_multiple_complete_traces() throws Exception {
        when(mockSink.isReady()).thenReturn(true);

        // Initialize and enable output manager
        outputManager.initialize(mockSink);
        outputManager.enable();

        // Create multiple events with live capture data and process them
        for (int i = 0; i < 3; i++) {
            Event liveEvent = JacksonEvent.fromMessage("{\"test\": \"data" + i + "\"}");
            LiveCaptureManager.setLiveCapture(liveEvent, true);
            LiveCaptureManager.addLiveCaptureEntry(liveEvent, LiveCaptureManager.SOURCE, "Source entry " + i, "source", null);
            LiveCaptureManager.addLiveCaptureEntry(liveEvent, LiveCaptureManager.SINK, "Sink entry " + i, "sink-" + i, null);

            // Get the live capture data and process it
            java.util.List<java.util.Map<String, Object>> liveCaptureData = LiveCaptureManager.getLiveCaptureOutput(liveEvent);
            outputManager.processEventLiveCapture(liveCaptureData);
        }

        // Verify sink received the entries (3 separate complete traces)
        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(mockSink, org.mockito.Mockito.times(3)).output(recordsCaptor.capture());

        // Each call should contain one complete trace
        java.util.List<Collection<Record<Event>>> allCalls = recordsCaptor.getAllValues();
        assertThat(allCalls.size(), equalTo(3));

        for (Collection<Record<Event>> call : allCalls) {
            assertThat(call.size(), equalTo(1)); // Each call has one complete trace
            Event traceEvent = call.iterator().next().getData();
            assertThat(traceEvent.getMetadata().getEventType(), equalTo("live-capture-trace"));

            // Verify trace structure - only entries should be present
            java.util.Map<String, Object> traceData = traceEvent.toMap();
            assertThat(traceData.containsKey("entries"), equalTo(true));
            assertThat(traceData.containsKey("entryCount"), equalTo(false)); // Should not contain entryCount
            assertThat(traceData.containsKey("captureId"), equalTo(false)); // Should not contain captureId
            assertThat(traceData.containsKey("timestamp"), equalTo(false)); // Should not contain timestamp
            assertThat(traceData.containsKey("traceId"), equalTo(false)); // Should not contain traceId
        }
    }

    @Test
    void processEventLiveCapture_handles_sink_errors_gracefully() throws Exception {
        when(mockSink.isReady()).thenReturn(true);
        doThrow(new RuntimeException("Sink processing error")).when(mockSink).output(any());

        outputManager.initialize(mockSink);
        outputManager.enable();

        // Create event with live capture data
        Event liveEvent = JacksonEvent.fromMessage("{\"test\": \"data\"}");
        LiveCaptureManager.setLiveCapture(liveEvent, true);
        LiveCaptureManager.addLiveCaptureEntry(liveEvent, LiveCaptureManager.SOURCE, "Source entry", "source", null);
        LiveCaptureManager.addLiveCaptureEntry(liveEvent, LiveCaptureManager.SINK, "Sink entry", "test-sink", null);

        // Get live capture data and process it (should handle errors gracefully)
        java.util.List<java.util.Map<String, Object>> liveCaptureData = LiveCaptureManager.getLiveCaptureOutput(liveEvent);

        // Should not throw exception despite sink error
        outputManager.processEventLiveCapture(liveCaptureData);

        verify(mockSink).output(any());
    }

    @Test
    void processEventLiveCapture_creates_proper_complete_trace_structure() throws Exception {
        when(mockSink.isReady()).thenReturn(true);

        outputManager.initialize(mockSink);
        outputManager.enable();

        // Create a live capture event
        Event liveEvent = JacksonEvent.builder()
            .withEventType("test")
            .withData(Map.of("field", "value"))
            .build();
        LiveCaptureManager.setLiveCapture(liveEvent, true);
        LiveCaptureManager.addLiveCaptureEntry(liveEvent, LiveCaptureManager.SOURCE, "Source entry", "source", null);
        LiveCaptureManager.addLiveCaptureEntry(liveEvent, LiveCaptureManager.SINK, "Test sink entry", "test-sink", null);

        // Get live capture data and process it
        java.util.List<java.util.Map<String, Object>> liveCaptureData = LiveCaptureManager.getLiveCaptureOutput(liveEvent);
        outputManager.processEventLiveCapture(liveCaptureData);

        ArgumentCaptor<Collection<Record<Event>>> recordsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(mockSink).output(recordsCaptor.capture());

        Record<Event> outputRecord = recordsCaptor.getValue().iterator().next();
        Event outputEvent = outputRecord.getData();

        // Verify the converted event has correct structure and metadata for complete trace
        assertThat(outputEvent.getMetadata().getEventType(), equalTo("live-capture-trace"));

        Map<String, Object> eventData = outputEvent.toMap();
        assertThat(eventData.containsKey("entries"), equalTo(true));
        assertThat(eventData.containsKey("entryCount"), equalTo(false)); // Should not contain entryCount
        assertThat(eventData.containsKey("traceId"), equalTo(false)); // Should not contain traceId
        assertThat(eventData.containsKey("captureId"), equalTo(false)); // Should not contain captureId
        assertThat(eventData.containsKey("timestamp"), equalTo(false)); // Should not contain timestamp

        // Verify entries array structure
        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> entries = (java.util.List<Map<String, Object>>) eventData.get("entries");
        assertThat(entries.size(), equalTo(2)); // Source + Sink

        // Check first entry (Source) - traceId no longer exists
        Map<String, Object> sourceEntry = entries.get(0);
        assertThat(sourceEntry.get("stage"), equalTo(LiveCaptureManager.SOURCE));
        assertThat(sourceEntry.containsKey("traceId"), equalTo(false)); // traceId no longer exists

        // Check second entry (Sink) - traceId no longer exists
        Map<String, Object> sinkEntry = entries.get(1);
        assertThat(sinkEntry.get("stage"), equalTo(LiveCaptureManager.SINK));
        assertThat(sinkEntry.containsKey("traceId"), equalTo(false)); // traceId no longer exists
        assertThat(sinkEntry.get("logMessage"), equalTo("Test sink entry"));

        // Verify captureMetaData exists and has expected structure
        @SuppressWarnings("unchecked")
        Map<String, Object> captureMetaData = (Map<String, Object>) sinkEntry.get("captureMetaData");
        assertThat(captureMetaData, hasKey("event"));
        assertThat(captureMetaData, hasKey("sinkName"));
        assertThat(captureMetaData.get("sinkName"), equalTo("test-sink"));
    }

    @Test
    void shutdown_cleans_up_resources() throws Exception {
        outputManager.initialize(mockSink);
        outputManager.enable();

        outputManager.shutdown();

        assertFalse(outputManager.isEnabled());
        verify(mockSink).shutdown();
    }
}