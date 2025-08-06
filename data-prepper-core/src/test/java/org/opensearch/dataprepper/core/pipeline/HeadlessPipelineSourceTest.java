/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;

import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.buffer.Buffer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import org.mockito.Mock;

import static org.awaitility.Awaitility.await;
import org.awaitility.core.ConditionTimeoutException;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

class HeadlessPipelineSourceTest {
    HeadlessPipelineSource headlessPipelineSource;

    @Mock
    private Buffer buffer;

    private HeadlessPipelineSource createObjectUnderTest() {
        final String name = UUID.randomUUID().toString();
        final String scope = UUID.randomUUID().toString();
        return new HeadlessPipelineSource(name, scope);
    }

    @BeforeEach
    void setup() {
        buffer = mock(Buffer.class);
    }

    @AfterEach
    void teardown() {
        if (headlessPipelineSource != null) {
            headlessPipelineSource.stop();
        }
    }

    @Test
    public void testSendEvents() throws Exception {
        doNothing().when(buffer).writeAll(any(Collection.class), any(Integer.class));
        headlessPipelineSource = createObjectUnderTest();
        headlessPipelineSource.start(buffer);
        Collection<Record<Event>> records = Collections.emptyList();
        headlessPipelineSource.sendEvents(records);
        verify(buffer).writeAll(eq(records), eq(HeadlessPipelineSource.DEFAULT_WRITE_TIMEOUT));
    }
    
    @Test
    public void testSendEventsException() throws Exception {
        doThrow(RuntimeException.class).when(buffer).writeAll(any(Collection.class), any(Integer.class));
        headlessPipelineSource = createObjectUnderTest();
        headlessPipelineSource.start(buffer);
        Record<Event> record = mock(Record.class);
        Event event = mock(Event.class);
        EventHandle eventHandle = mock(EventHandle.class);
        when(record.getData()).thenReturn(event);
        when(event.getEventHandle()).thenReturn(eventHandle);
        Collection<Record<Event>> records = Collections.singletonList(record);
        headlessPipelineSource.sendEvents(records);
        verify(eventHandle).release(eq(false));

    }

    @Test
    //@Timeout(value = 2000, unit = TimeUnit.MILLISECONDS)
    public void testSendEventsExceptionWithAcknowledgements() throws Exception {
        doThrow(RuntimeException.class).when(buffer).writeAll(any(Collection.class), any(Integer.class));
        headlessPipelineSource = createObjectUnderTest();
        headlessPipelineSource.setAcknowledgementsEnabled(true);
        headlessPipelineSource.start(buffer);
        Record<Event> record = mock(Record.class);
        Event event = mock(Event.class);
        EventHandle eventHandle = mock(EventHandle.class);
        when(record.getData()).thenReturn(event);
        when(event.getEventHandle()).thenReturn(eventHandle);
        Collection<Record<Event>> records = Collections.singletonList(record);
        assertThrows(ConditionTimeoutException.class, () -> {
        await().atMost(2000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> {
                headlessPipelineSource.sendEvents(records);
            });
        });
        assertThat(headlessPipelineSource.getNumberOfSuccessfulEvents(), equalTo(0L));
        assertThat(headlessPipelineSource.getNumberOfFailedEvents(), equalTo(0L));
        verify(eventHandle, never()).release(eq(false));

    }

}


