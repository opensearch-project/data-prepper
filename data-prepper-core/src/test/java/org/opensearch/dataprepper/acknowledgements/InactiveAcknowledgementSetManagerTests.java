/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.Event;

import java.time.Duration;

public class InactiveAcknowledgementSetManagerTests {
    InactiveAcknowledgementSetManager acknowledgementSetManager;

    @BeforeEach
    void setup() {
        acknowledgementSetManager = InactiveAcknowledgementSetManager.getInstance();
    }

    @Test
    void testCreateAPI() {
        assertThat(acknowledgementSetManager, notNullValue());
        assertThrows(UnsupportedOperationException.class, () -> acknowledgementSetManager.create((a)->{}, Duration.ofMillis(10)));
    }

    @Test
    void testEventAcquireAPI() {
        assertThat(acknowledgementSetManager, notNullValue());
        Event event = mock(Event.class);
        assertThrows(UnsupportedOperationException.class, () -> acknowledgementSetManager.acquireEventReference(event));
    }

    @Test
    void testEventHandleAcquireAPI() {
        assertThat(acknowledgementSetManager, notNullValue());
        EventHandle eventHandle = mock(EventHandle.class);
        assertThrows(UnsupportedOperationException.class, () -> acknowledgementSetManager.acquireEventReference(eventHandle));
    }

    @Test
    void testReleaseAPI() {
        assertThat(acknowledgementSetManager, notNullValue());
        EventHandle eventHandle = mock(EventHandle.class);
        assertThrows(UnsupportedOperationException.class, () -> acknowledgementSetManager.releaseEventReference(eventHandle, true));
    }

}
