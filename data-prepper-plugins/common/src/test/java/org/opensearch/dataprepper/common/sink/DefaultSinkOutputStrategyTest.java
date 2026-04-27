/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;

import org.mockito.Mock;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyDouble;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DefaultSinkOutputStrategyTest {
    @Mock
    private LockStrategy lockStrategy;
    @Mock
    private SinkBuffer sinkBuffer;
    @Mock
    private SinkFlushContext sinkFlushContext;
    @Mock
    private SinkFlushableBuffer flushableBuffer;
    @Mock
    private SinkFlushResult flushResult;
    @Mock
    private SinkMetrics sinkMetrics;

    private List<Event> eventsInBuffer;
    private boolean flushed;

    private DefaultSinkOutputStrategy sinkOutputStrategy;


    @BeforeEach
    void setUp() throws Exception {
        flushed = false;
        eventsInBuffer = new ArrayList<>();
        lockStrategy = mock(LockStrategy.class);
        sinkBuffer = mock(SinkBuffer.class);
        sinkMetrics = mock(SinkMetrics.class);
        flushResult = mock(SinkFlushResult.class);
        sinkFlushContext = mock(SinkFlushContext.class);
        flushableBuffer = mock(SinkFlushableBuffer.class);
        doAnswer(a -> {
            TestSinkBufferEntry bufferEntry = (TestSinkBufferEntry)a.getArgument(0);
            if (flushed) {
                flushed = false;
                eventsInBuffer.clear();
            }
            eventsInBuffer.add(bufferEntry.getEvent());
            return true;
        }).when(sinkBuffer).addToBuffer(any(SinkBufferEntry.class));
        when(flushableBuffer.getEvents()).thenReturn(eventsInBuffer);
        when(sinkBuffer.isMaxEventsLimitReached()).thenReturn(false);
        doAnswer(a -> {
            TestSinkBufferEntry bufferEntry = (TestSinkBufferEntry)a.getArgument(0);
            return (bufferEntry.getEstimatedSize() > 100);
        }).when(sinkBuffer).willExceedMaxRequestSizeBytes(any());

        when(sinkBuffer.exceedsFlushTimeInterval()).thenReturn(false);

        when(sinkBuffer.getFlushableBuffer(any())).thenReturn(flushableBuffer);
    }

    private DefaultSinkOutputStrategy createObjectUnderTest() {
        return new DefaultSinkOutputStrategyImpl(lockStrategy, sinkBuffer, sinkFlushContext, sinkMetrics);
    }

    @Test
    public void test_execution_with_successful_events_flush() {
        when(flushableBuffer.flush()).thenReturn(null);
        sinkOutputStrategy = createObjectUnderTest();
        List<Record<Event>> records = new ArrayList<>();
        Event event1 = mock(Event.class);
        Event event2 = mock(Event.class);
        EventHandle eventHandle1 = mock(EventHandle.class);
        EventHandle eventHandle2 = mock(EventHandle.class);
        records.add(new Record<>(event1));
        records.add(new Record<>(event2));
        when(event1.getEventHandle()).thenReturn(eventHandle1);
        when(event2.getEventHandle()).thenReturn(eventHandle2);
        when(event1.get(eq("size"), any())).thenReturn(10L);
        when(event2.get(eq("size"), any())).thenReturn(10L);
        sinkOutputStrategy.execute(records);
        when(sinkBuffer.exceedsFlushTimeInterval()).thenReturn(true);
        sinkOutputStrategy.execute(Collections.emptyList());

        verify(sinkBuffer).getFlushableBuffer(any());
        verify(sinkMetrics).recordRequestLatency(anyLong(), eq(TimeUnit.NANOSECONDS));
        verify(eventHandle1).release(true);
        verify(eventHandle2).release(true);
    }

    @Test
    public void test_execution_with_successful_events_with_multiple_flushes_when_request_limit_exceeds() {
        doAnswer(a -> {
            flushed = true;
            return null;
        }).when(flushableBuffer).flush();
        sinkOutputStrategy = createObjectUnderTest();
        List<Record<Event>> records = new ArrayList<>();
        Event event1 = mock(Event.class);
        Event event2 = mock(Event.class);
        EventHandle eventHandle1 = mock(EventHandle.class);
        EventHandle eventHandle2 = mock(EventHandle.class);
        records.add(new Record<>(event1));
        records.add(new Record<>(event2));
        when(event1.getEventHandle()).thenReturn(eventHandle1);
        when(event2.getEventHandle()).thenReturn(eventHandle2);
        when(event1.get(eq("size"), any())).thenReturn(10L);
        when(event2.get(eq("size"), any())).thenReturn(300L);
        sinkOutputStrategy.execute(records);
        when(sinkBuffer.exceedsFlushTimeInterval()).thenReturn(true);
        sinkOutputStrategy.execute(Collections.emptyList());

        verify(sinkBuffer, times(2)).getFlushableBuffer(any());
        verify(sinkMetrics, times(2)).recordRequestLatency(anyLong(), eq(TimeUnit.NANOSECONDS));
        verify(eventHandle1).release(true);
        verify(eventHandle2).release(true);
    }

    @Test
    public void test_execution_with_successful_events_with_multiple_flushes_when_events_limit_exceeds() {
        when(sinkBuffer.isMaxEventsLimitReached()).thenReturn(true);
        doAnswer(a -> {
            flushed = true;
            return null;
        }).when(flushableBuffer).flush();
        sinkOutputStrategy = createObjectUnderTest();
        List<Record<Event>> records = new ArrayList<>();
        Event event1 = mock(Event.class);
        Event event2 = mock(Event.class);
        EventHandle eventHandle1 = mock(EventHandle.class);
        EventHandle eventHandle2 = mock(EventHandle.class);
        records.add(new Record<>(event1));
        records.add(new Record<>(event2));
        when(event1.getEventHandle()).thenReturn(eventHandle1);
        when(event2.getEventHandle()).thenReturn(eventHandle2);
        when(event1.get(eq("size"), any())).thenReturn(10L);
        when(event2.get(eq("size"), any())).thenReturn(10L);
        sinkOutputStrategy.execute(records);
        when(sinkBuffer.exceedsFlushTimeInterval()).thenReturn(true);

        verify(sinkBuffer, times(2)).getFlushableBuffer(any());
        verify(sinkMetrics, times(2)).recordRequestLatency(anyLong(), eq(TimeUnit.NANOSECONDS));
        verify(eventHandle1).release(true);
        verify(eventHandle2).release(true);
    }

    @Test
    public void test_execution_with_one_large_event() {
        when(flushableBuffer.flush()).thenReturn(null);
        sinkOutputStrategy = createObjectUnderTest();
        List<Record<Event>> records = new ArrayList<>();
        Event event1 = mock(Event.class);
        Event event2 = mock(Event.class);
        EventHandle eventHandle1 = mock(EventHandle.class);
        EventHandle eventHandle2 = mock(EventHandle.class);
        records.add(new Record<>(event1));
        records.add(new Record<>(event2));
        when(event1.get(eq("size"), any())).thenReturn(10L);
        when(event2.get(eq("size"), any())).thenReturn(100000L);
        when(event1.getEventHandle()).thenReturn(eventHandle1);
        when(event2.getEventHandle()).thenReturn(eventHandle2);
        sinkOutputStrategy.execute(records);
        when(sinkBuffer.exceedsFlushTimeInterval()).thenReturn(true);
        sinkOutputStrategy.execute(Collections.emptyList());

        verify(sinkBuffer).getFlushableBuffer(any());
        verify(sinkMetrics).recordRequestLatency(anyLong(), eq(TimeUnit.NANOSECONDS));
        verify(eventHandle1).release(true);
        verify(eventHandle2).release(false);
    }

    @Test
    public void test_execution_when_get_buffer_entry_throws_exception() {
        when(flushableBuffer.flush()).thenReturn(null);
        sinkOutputStrategy = createObjectUnderTest();
        List<Record<Event>> records = new ArrayList<>();
        Event event1 = mock(Event.class);
        Event event2 = mock(Event.class);
        EventHandle eventHandle1 = mock(EventHandle.class);
        EventHandle eventHandle2 = mock(EventHandle.class);
        records.add(new Record<>(event1));
        records.add(new Record<>(event2));
        when(event1.get(eq("size"), any())).thenReturn(10L);
        when(event2.get(eq("size"), any())).thenReturn(-1L);
        when(event1.getEventHandle()).thenReturn(eventHandle1);
        when(event2.getEventHandle()).thenReturn(eventHandle2);
        sinkOutputStrategy.execute(records);
        when(sinkBuffer.exceedsFlushTimeInterval()).thenReturn(true);
        sinkOutputStrategy.execute(Collections.emptyList());

        verify(sinkBuffer).getFlushableBuffer(any());
        verify(sinkMetrics).recordRequestLatency(anyLong(), eq(TimeUnit.NANOSECONDS));
        verify(eventHandle1).release(true);
        verify(eventHandle2).release(false);
    }

    @Test
    public void test_execution_with_failed_events_flush() {
        when(flushResult.getEvents()).thenReturn(eventsInBuffer);
        when(flushableBuffer.flush()).thenReturn(flushResult);
        sinkOutputStrategy = createObjectUnderTest();
        List<Record<Event>> records = new ArrayList<>();
        Event event1 = mock(Event.class);
        Event event2 = mock(Event.class);
        EventHandle eventHandle1 = mock(EventHandle.class);
        EventHandle eventHandle2 = mock(EventHandle.class);
        records.add(new Record<>(event1));
        records.add(new Record<>(event2));
        when(event1.get(eq("size"), any())).thenReturn(10L);
        when(event2.get(eq("size"), any())).thenReturn(10L);
        when(event1.getEventHandle()).thenReturn(eventHandle1);
        when(event2.getEventHandle()).thenReturn(eventHandle2);
        sinkOutputStrategy.execute(records);
        when(sinkBuffer.exceedsFlushTimeInterval()).thenReturn(true);
        sinkOutputStrategy.execute(Collections.emptyList());

        verify(sinkBuffer).getFlushableBuffer(any());
        verify(sinkMetrics, times(0)).recordRequestLatency(anyDouble());
        verify(eventHandle1).release(false);
        verify(eventHandle2).release(false);
    }

    @Test
    public void test_execution_with_exception_during_flush() {
        when(flushResult.getEvents()).thenReturn(eventsInBuffer);
        when(flushableBuffer.flush()).thenThrow(new RuntimeException("exception"));
        sinkOutputStrategy = createObjectUnderTest();
        List<Record<Event>> records = new ArrayList<>();
        Event event1 = mock(Event.class);
        Event event2 = mock(Event.class);
        EventHandle eventHandle1 = mock(EventHandle.class);
        EventHandle eventHandle2 = mock(EventHandle.class);
        records.add(new Record<>(event1));
        records.add(new Record<>(event2));
        when(event1.getEventHandle()).thenReturn(eventHandle1);
        when(event2.getEventHandle()).thenReturn(eventHandle2);
        when(event1.get(eq("size"), any())).thenReturn(10L);
        when(event2.get(eq("size"), any())).thenReturn(10L);
        sinkOutputStrategy.execute(records);
        when(sinkBuffer.exceedsFlushTimeInterval()).thenReturn(true);
        sinkOutputStrategy.execute(Collections.emptyList());

        verify(sinkBuffer).getFlushableBuffer(any());
        verify(sinkMetrics, times(0)).recordRequestLatency(anyDouble());
        verify(eventHandle1).release(false);
        verify(eventHandle2).release(false);
    }

    private static class TestSinkBufferEntry implements SinkBufferEntry {
        private final Event event;
        private final long estimatedSize;
        public TestSinkBufferEntry(final Event event, long estimatedSize) {
            this.event = event;
            this.estimatedSize = estimatedSize;
        }
        public long getEstimatedSize() {
            return estimatedSize;
        }

        public Event getEvent() {
            return event;
        }
        public boolean exceedsMaxEventSizeThreshold() {
            return estimatedSize > 1000L;
        }
    }

    private static class DefaultSinkOutputStrategyImpl extends DefaultSinkOutputStrategy {
        
        private List<Event> dlqEvents;

        public DefaultSinkOutputStrategyImpl(final LockStrategy lockStrategy, final SinkBuffer sinkBuffer, final SinkFlushContext sinkFlushContext, final SinkMetrics sinkMetrics) {
            super(lockStrategy, sinkBuffer, sinkFlushContext, sinkMetrics);
            dlqEvents = new ArrayList<>();
        }

        public void flushDlqList() {
            for (final Event event: dlqEvents) {
                event.getEventHandle().release(false);
            }
            dlqEvents.clear();
        }

        public void addFailedEventsToDlq(final List<Event> events, final Throwable ex, final int statusCode) {
            dlqEvents.addAll(events);
        }

        public SinkBufferEntry getSinkBufferEntry(final Event event) throws Exception {
            long size = event.get("size", Long.class);
            if (size < 0) {
                throw new RuntimeException("invalid size");
            }
            return new TestSinkBufferEntry(event, size);
        }

    }

}
