/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AggregateResponseEventHandlingStrategyTest {


    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Mock
    private Event originalEvent;

    @Mock
    private DefaultEventHandle eventHandle;

    @Mock
    private Event parsedEvent1;

    @Mock
    private Event parsedEvent2;

    private List<Record<Event>> originalRecords;

    private AggregateResponseEventHandlingStrategy aggregateResponseEventHandlingStrategy;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        aggregateResponseEventHandlingStrategy = new AggregateResponseEventHandlingStrategy();

        // Set up original records list with a mock original event
        originalRecords = new ArrayList<>();
        originalRecords.add(new Record<>(originalEvent));

        // Mock event handle and acknowledgement set
        when(originalEvent.getEventHandle()).thenReturn(eventHandle);
        when(eventHandle.getAcknowledgementSet()).thenReturn(acknowledgementSet);
    }

    @Test
    public void testHandleEvents_AddsParsedEventsToResultRecords() {
        // Arrange
        List<Event> parsedEvents = Arrays.asList(parsedEvent1, parsedEvent2);

        // Act
        List<Record<Event>> resultRecords = aggregateResponseEventHandlingStrategy.handleEvents(parsedEvents, originalRecords);

        // Assert
        assertEquals(2, resultRecords.size());
        assertEquals(parsedEvent1, resultRecords.get(0).getData());
        assertEquals(parsedEvent2, resultRecords.get(1).getData());

        // Verify that the parsed events are added to the acknowledgement set
        verify(acknowledgementSet, times(1)).add(parsedEvent1);
        verify(acknowledgementSet, times(1)).add(parsedEvent2);
    }

    @Test
    public void testHandleEvents_NoAcknowledgementSet_DoesNotThrowException() {
        // Arrange
        List<Event> parsedEvents = Arrays.asList(parsedEvent1, parsedEvent2);

        // Mock eventHandle to return null for the acknowledgement set
        when(eventHandle.getAcknowledgementSet()).thenReturn(null);

        // Act
        List<Record<Event>> resultRecords = aggregateResponseEventHandlingStrategy.handleEvents(parsedEvents, originalRecords);

        // Assert
        assertEquals(2, resultRecords.size());
        assertEquals(parsedEvent1, resultRecords.get(0).getData());
        assertEquals(parsedEvent2, resultRecords.get(1).getData());

        // Verify that no events are added to the acknowledgement set
        verify(acknowledgementSet, never()).add(any(Event.class));
    }

    @Test
    public void testHandleEvents_EmptyParsedEvents_DoesNotAddToResultRecords() {
        // Arrange
        List<Event> parsedEvents = new ArrayList<>();

        // Act
        List<Record<Event>> resultRecords = aggregateResponseEventHandlingStrategy.handleEvents(parsedEvents, originalRecords);

        // Assert
        assertEquals(0, resultRecords.size());

        // Verify that no events are added to the acknowledgement set
        verify(acknowledgementSet, never()).add(any(Event.class));
    }
}

