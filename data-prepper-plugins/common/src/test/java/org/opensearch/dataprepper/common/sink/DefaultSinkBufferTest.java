/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.Mock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

public class DefaultSinkBufferTest {
    @Mock
    private SinkBufferWriter sinkBufferWriter;
    @Mock
    private SinkFlushableBuffer sinkFlushableBuffer;
    @Mock
    private SinkBufferEntry sinkBufferEntry;
    @Mock
    private SinkFlushContext sinkFlushContext;

    private long maxEvents;
    private long maxRequestSize;
    private long flushIntervalMs;
    private DefaultSinkBuffer defaultSinkBuffer;

    @BeforeEach
    void setUp() {
        sinkBufferWriter = mock(SinkBufferWriter.class);
        sinkFlushableBuffer = mock(SinkFlushableBuffer.class);
        sinkBufferEntry = mock(SinkBufferEntry.class);
        sinkFlushContext = mock(SinkFlushContext.class);
        when(sinkBufferWriter.getBuffer(any())).thenReturn(sinkFlushableBuffer);
    }

    private DefaultSinkBuffer createObjectUnderTest() {
        return new DefaultSinkBuffer(maxEvents, maxRequestSize, flushIntervalMs, sinkBufferWriter);
    }

    @Test
    public void test_buffer_creation_and_adding_entry() throws Exception {
        maxEvents = 2L;
        maxRequestSize = 10000L;
        flushIntervalMs = 500000L;
        defaultSinkBuffer = createObjectUnderTest();
        assertThat(defaultSinkBuffer.getFlushableBuffer(sinkFlushContext), sameInstance(sinkFlushableBuffer));
        assertFalse(defaultSinkBuffer.isMaxEventsLimitReached());        
        when(sinkBufferEntry.getEstimatedSize()).thenReturn(10L);
        assertFalse(defaultSinkBuffer.willExceedMaxRequestSizeBytes(sinkBufferEntry));        
        assertFalse(defaultSinkBuffer.exceedsFlushTimeInterval());        
        when(sinkBufferWriter.writeToBuffer(any())).thenReturn(true);

        assertTrue(defaultSinkBuffer.addToBuffer(sinkBufferEntry));
    }

    @Test
    public void test_buffer_creation_and_check_event_limits() throws Exception {
        maxEvents = 2L;
        maxRequestSize = 10000L;
        flushIntervalMs = 500000L;
        defaultSinkBuffer = createObjectUnderTest();
        assertFalse(defaultSinkBuffer.isMaxEventsLimitReached());        
        when(sinkBufferEntry.getEstimatedSize()).thenReturn(20000L);
        assertTrue(defaultSinkBuffer.willExceedMaxRequestSizeBytes(sinkBufferEntry));        
        when(sinkBufferEntry.getEstimatedSize()).thenReturn(10L);
        assertFalse(defaultSinkBuffer.willExceedMaxRequestSizeBytes(sinkBufferEntry));        
        assertFalse(defaultSinkBuffer.exceedsFlushTimeInterval());        
        when(sinkBufferWriter.writeToBuffer(any())).thenReturn(true);

        assertTrue(defaultSinkBuffer.addToBuffer(sinkBufferEntry));
        assertTrue(defaultSinkBuffer.addToBuffer(sinkBufferEntry));
        assertTrue(defaultSinkBuffer.isMaxEventsLimitReached());        
    }

    @Test
    public void test_buffer_creation_and_check_flush_interval() throws Exception {
        maxEvents = 2L;
        maxRequestSize = 10000L;
        flushIntervalMs = 5L;
        defaultSinkBuffer = createObjectUnderTest();
        assertFalse(defaultSinkBuffer.isMaxEventsLimitReached());        
        try {
            Thread.sleep(10);
        } catch(Exception e) {}
        assertTrue(defaultSinkBuffer.exceedsFlushTimeInterval());
    }

    @Test
    public void test_add_buffer_failure() throws Exception {
        maxEvents = 2L;
        maxRequestSize = 10000L;
        flushIntervalMs = 500000L;
        defaultSinkBuffer = createObjectUnderTest();
        assertFalse(defaultSinkBuffer.isMaxEventsLimitReached());        
        when(sinkBufferEntry.getEstimatedSize()).thenReturn(10L);
        when(sinkBufferWriter.writeToBuffer(any())).thenReturn(false);

        assertFalse(defaultSinkBuffer.addToBuffer(sinkBufferEntry));
        assertFalse(defaultSinkBuffer.isMaxEventsLimitReached());        
    }
}

