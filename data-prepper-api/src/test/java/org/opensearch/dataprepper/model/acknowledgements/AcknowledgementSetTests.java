/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.acknowledgements;

import org.junit.jupiter.api.Test;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.junit.jupiter.api.BeforeEach;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;

public class AcknowledgementSetTests {
    
    Event event;
    EventHandle eventHandle;
    AcknowledgementSet acknowledgementSet;

    @BeforeEach
    public void setup() {
        event = mock(Event.class);
        eventHandle = mock(EventHandle.class);
        when(event.getEventHandle()).thenReturn(eventHandle);
        acknowledgementSet = spy(AcknowledgementSet.class);
    }

    @Test
    public void testAcknowledgementSetAdd() {
        acknowledgementSet.add(event);
        verify(acknowledgementSet).add(eventHandle);
    }
}

