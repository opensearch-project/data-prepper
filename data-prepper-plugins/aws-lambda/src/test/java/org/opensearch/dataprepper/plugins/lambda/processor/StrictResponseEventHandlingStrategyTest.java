/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.lambda.processor.exception.StrictResponseModeNotRespectedException;
import static org.opensearch.dataprepper.plugins.lambda.utils.LambdaTestSetupUtil.getSampleEventRecords;
import static org.opensearch.dataprepper.plugins.lambda.utils.LambdaTestSetupUtil.getSampleParsedEvents;

import java.util.ArrayList;
import java.util.List;

public class StrictResponseEventHandlingStrategyTest {


    private final StrictResponseEventHandlingStrategy strictResponseEventHandlingStrategy = new StrictResponseEventHandlingStrategy();

    @Test
    public void testHandleEvents_WithMatchingEventCount_ShouldUpdateOriginalEvents() {

        // Arrange
        int oneRandomCount = (int) (Math.random() * 100);
        List<Event> parsedEvents = getSampleParsedEvents(oneRandomCount);
        List<Record<Event>> originalRecords = getSampleEventRecords(oneRandomCount);

        // Before Test, make sure that they are not the same
        for (int i = 0; i < oneRandomCount; i++) {
            assertNotEquals(originalRecords.get(i).getData(), parsedEvents.get(i));
        }

        // Act
        List<Record<Event>> resultRecords = strictResponseEventHandlingStrategy.handleEvents(parsedEvents, originalRecords);

        // Before Test, make sure that they are not the same
        for (int i = 0; i < oneRandomCount; i++) {
            assertNotEquals(resultRecords.get(i).getData(), parsedEvents.get(i));
        }
    }

    @Test
    public void testHandleEvents_WithMismatchingEventCount_ShouldThrowException() {
        // Arrange
        int firstRandomCount = (int) (Math.random() * 10);
        List<Event> parsedEvents = getSampleParsedEvents(firstRandomCount);
        List<Record<Event>> originalRecords = getSampleEventRecords(firstRandomCount + 10);

        // Act & Assert
        RuntimeException exception = assertThrows(StrictResponseModeNotRespectedException.class, () ->
                strictResponseEventHandlingStrategy.handleEvents(parsedEvents, originalRecords)
        );
    }

    @Test
    public void testHandleEvents_EmptyParsedEvents_ShouldNotThrowException() {
        // Act
        List<Record<Event>> resultRecords = strictResponseEventHandlingStrategy.handleEvents(new ArrayList<>(), new ArrayList<>());
        // Ensure resultRecords is empty
        assertEquals(0, resultRecords.size());
    }
}

