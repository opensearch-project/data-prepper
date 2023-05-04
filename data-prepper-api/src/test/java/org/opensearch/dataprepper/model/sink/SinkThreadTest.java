/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SinkThreadTest {
    @Mock
    AbstractSink sink;

    SinkThread sinkThread;

    @Test
    public void testSinkThread() {
        when(sink.isReady()).thenReturn(true);
        sinkThread = new SinkThread(sink, 5);
        sinkThread.run();
        verify(sink, times(1)).isReady();
    }

    @Test
    public void testSinkThread2() {
        when(sink.isReady()).thenReturn(false);
        sinkThread = new SinkThread(sink, 5);
        sinkThread.run();
        verify(sink, times(6)).isReady();
        try {
            doAnswer((i) -> {
                return null;
            }).when(sink).doInitialize();
            verify(sink, times(5)).doInitialize();
        } catch (Exception e){}
        when(sink.isReady()).thenReturn(false).thenReturn(true);
        sinkThread.run();
        verify(sink, times(8)).isReady();
        when(sink.isReady()).thenReturn(false).thenReturn(true);
        try {
            lenient().doAnswer((i) -> {
                throw new InterruptedException("Fake interrupt");
            }).when(sink).doInitialize();
            sinkThread.run();
            verify(sink, times(7)).doInitialize();
        } catch (Exception e){}
    }
}
