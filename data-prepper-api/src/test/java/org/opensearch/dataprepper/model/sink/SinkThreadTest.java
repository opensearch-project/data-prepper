/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.sink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SinkThreadTest {
    @Mock
    private AbstractSink sink;

    private SinkThread sinkThread;

    @Test
    public void testSinkThread() {
        when(sink.isReady()).thenReturn(true);
        sinkThread = new SinkThread(sink, 5, 100);
        sinkThread.run();
        verify(sink, times(1)).isReady();
    }

    @Test
    public void testSinkThread2() {
        when(sink.isReady()).thenReturn(false);
        sinkThread = new SinkThread(sink, 5, 100);
        sinkThread.run();
        verify(sink, times(11)).isReady();
        verify(sink, times(5)).doInitialize();
        when(sink.isReady()).thenReturn(false).thenReturn(false).thenReturn(true);
        sinkThread.run();
        verify(sink, times(14)).isReady();
        when(sink.isReady()).thenReturn(false).thenReturn(false).thenReturn(true);
        lenient().doAnswer((i) -> {
            throw new InterruptedException("Fake interrupt");
        }).when(sink).doInitialize();
        sinkThread.run();
        verify(sink, times(7)).doInitialize();
    }

    @Test
    public void run_when_sink_becomes_ready_while_waiting_does_not_call_doInitialize() {
        when(sink.isReady()).thenReturn(false).thenReturn(true);
        sinkThread = new SinkThread(sink, 5, 10);
        sinkThread.run();
        verify(sink, never()).doInitialize();
    }
}
