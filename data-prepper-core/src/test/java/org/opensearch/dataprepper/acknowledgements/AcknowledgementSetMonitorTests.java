/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.opensearch.dataprepper.event.DefaultEventHandle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;
import static org.hamcrest.Matchers.equalTo;

import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
public class AcknowledgementSetMonitorTests {
    private static final int DEFAULT_WAIT_TIME_MS = 500;
    @Mock
    DefaultAcknowledgementSet acknowledgementSet1;
    @Mock
    DefaultAcknowledgementSet acknowledgementSet2;
    @Mock
    DefaultEventHandle eventHandle1;

    private AcknowledgementSetMonitor acknowledgementSetMonitor;

    AcknowledgementSetMonitor createObjectUnderTest() {
        return new AcknowledgementSetMonitor();
    }

    @BeforeEach
    void setup() {
        acknowledgementSet1 = mock(DefaultAcknowledgementSet.class);
        eventHandle1 = mock(DefaultEventHandle.class);
        when(acknowledgementSet1.isDone()).thenReturn(true);
        acknowledgementSetMonitor = createObjectUnderTest();
    }

    @Test
    public void testBasic() {
        acknowledgementSetMonitor.add(acknowledgementSet1);
        Thread shutdownThread = new Thread(() -> {
            try {
                Thread.sleep(DEFAULT_WAIT_TIME_MS);
            } catch (Exception e){}
        });
        shutdownThread.start();
        acknowledgementSetMonitor.run();
        assertThat(acknowledgementSetMonitor.getSize(), equalTo(0));
    }

    @Test
    public void testMultipleAcknowledgementSets() {
        acknowledgementSet2 = mock(DefaultAcknowledgementSet.class);
        when(acknowledgementSet2.isDone()).thenReturn(false);

        acknowledgementSetMonitor.add(acknowledgementSet1);
        acknowledgementSetMonitor.add(acknowledgementSet2);
        Thread shutdownThread = new Thread(() -> {
            try {
                Thread.sleep(DEFAULT_WAIT_TIME_MS);
            } catch (Exception e){}
        });
        shutdownThread.start();
        acknowledgementSetMonitor.run();
        assertThat(acknowledgementSetMonitor.getSize(), equalTo(1));
    }

    @Test
    public void testAcknowledgementSetAcquireRelease() {
        when(eventHandle1.getAcknowledgementSet()).thenReturn(acknowledgementSet1);
        try {
            doAnswer((i) -> {return null; }).when(acknowledgementSet1).acquire(eventHandle1);
        } catch (Exception e){}
        acknowledgementSetMonitor.add(acknowledgementSet1);
        acknowledgementSetMonitor.acquire(eventHandle1);
        acknowledgementSetMonitor.release(eventHandle1, true);
        Thread shutdownThread = new Thread(() -> {
            try {
                Thread.sleep(DEFAULT_WAIT_TIME_MS);
            } catch (Exception e){}
        });
        shutdownThread.start();
        acknowledgementSetMonitor.run();
        assertThat(acknowledgementSetMonitor.getSize(), equalTo(0));
    }

    @Test
    public void testAcknowledgementSetInvalidAcquire() {
        acknowledgementSet2 = mock(DefaultAcknowledgementSet.class);
        when(eventHandle1.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        acknowledgementSetMonitor.add(acknowledgementSet1);
        acknowledgementSetMonitor.acquire(eventHandle1);
        Thread shutdownThread = new Thread(() -> {
            try {
                Thread.sleep(DEFAULT_WAIT_TIME_MS);
            } catch (Exception e){}
        });
        shutdownThread.start();
        acknowledgementSetMonitor.run();
        assertThat(acknowledgementSetMonitor.getSize(), equalTo(0));
        assertThat(acknowledgementSetMonitor.getNumInvalidAcquires(), equalTo(1));
    }

    @Test
    public void testAcknowledgementSetInvalidRelease() {
        acknowledgementSet2 = mock(DefaultAcknowledgementSet.class);
        when(eventHandle1.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        acknowledgementSetMonitor.add(acknowledgementSet1);
        acknowledgementSetMonitor.release(eventHandle1, true);
        Thread shutdownThread = new Thread(() -> {
            try {
                Thread.sleep(DEFAULT_WAIT_TIME_MS);
            } catch (Exception e){}
        });
        shutdownThread.start();
        acknowledgementSetMonitor.run();
        assertThat(acknowledgementSetMonitor.getSize(), equalTo(0));
        assertThat(acknowledgementSetMonitor.getNumInvalidReleases(), equalTo(1));
    }
}
