package org.opensearch.dataprepper.plugins.lambda.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StrictResponseEventHandlingStrategyTest {

    @Mock
    private Buffer flushedBuffer;

    @Mock
    private Event originalEvent;

    @Mock
    private Event parsedEvent1;

    @Mock
    private Event parsedEvent2;

    private List<Record<Event>> originalRecords;
    private List<Record<Event>> resultRecords;
    private StrictResponseEventHandlingStrategy strictResponseEventHandlingStrategy;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        strictResponseEventHandlingStrategy = new StrictResponseEventHandlingStrategy();

        // Set up original records list with mock original events
        originalRecords = new ArrayList<>();
        resultRecords = new ArrayList<>();
        originalRecords.add(new Record<>(originalEvent));
        originalRecords.add(new Record<>(originalEvent));
    }

    @Test
    public void testHandleEvents_WithMatchingEventCount_ShouldUpdateOriginalEvents() {
        // Arrange
        List<Event> parsedEvents = Arrays.asList(parsedEvent1, parsedEvent2);

        // Mocking flushedBuffer to return an event count of 2
        when(flushedBuffer.getEventCount()).thenReturn(2);

        // Mocking parsedEvent1 and parsedEvent2 to return sample data
        Map<String, Object> responseData1 = new HashMap<>();
        responseData1.put("key1", "value1");
        when(parsedEvent1.toMap()).thenReturn(responseData1);

        Map<String, Object> responseData2 = new HashMap<>();
        responseData2.put("key2", "value2");
        when(parsedEvent2.toMap()).thenReturn(responseData2);

        // Act
        strictResponseEventHandlingStrategy.handleEvents(parsedEvents, originalRecords, resultRecords, flushedBuffer);

        // Assert
        // Verify original event is cleared and then updated with response data
        verify(originalEvent, times(2)).clear();
        verify(originalEvent).put("key1", "value1");
        verify(originalEvent).put("key2", "value2");

        // Ensure resultRecords contains the original records
        assertEquals(2, resultRecords.size());
        assertEquals(originalRecords.get(0), resultRecords.get(0));
        assertEquals(originalRecords.get(1), resultRecords.get(1));
    }

    @Test
    public void testHandleEvents_WithMismatchingEventCount_ShouldThrowException() {
        // Arrange
        List<Event> parsedEvents = Arrays.asList(parsedEvent1, parsedEvent2);

        // Mocking flushedBuffer to return an event count of 3 (mismatch)
        when(flushedBuffer.getEventCount()).thenReturn(3);

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
                strictResponseEventHandlingStrategy.handleEvents(parsedEvents, originalRecords, resultRecords, flushedBuffer)
        );

        assertEquals("Response Processing Mode is configured as Strict mode but behavior is aggregate mode. Event count mismatch.", exception.getMessage());

        // Verify original events were not cleared or modified
        verify(originalEvent, never()).clear();
        verify(originalEvent, never()).put(anyString(), any());
    }

    @Test
    public void testHandleEvents_EmptyParsedEvents_ShouldNotThrowException() {
        // Arrange
        List<Event> parsedEvents = new ArrayList<>();

        // Mocking flushedBuffer to return an event count of 0
        when(flushedBuffer.getEventCount()).thenReturn(0);

        // Act
        strictResponseEventHandlingStrategy.handleEvents(parsedEvents, originalRecords, resultRecords, flushedBuffer);

        // Assert
        // Verify no events were cleared or modified
        verify(originalEvent, never()).clear();
        verify(originalEvent, never()).put(anyString(), any());

        // Ensure resultRecords is empty
        assertEquals(0, resultRecords.size());
    }
}

