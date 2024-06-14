/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import org.mockito.Mock;

import java.lang.ref.WeakReference;
import java.time.Instant;

class AggregateEventHandleTests {
    @Mock
    private AcknowledgementSet acknowledgementSet1;
    @Mock
    private AcknowledgementSet acknowledgementSet2;
    private int count;

    @Test
    void testBasic() {
        Instant now = Instant.now();
        AggregateEventHandle eventHandle = new AggregateEventHandle(now);
        assertThat(eventHandle.getInternalOriginationTime(), equalTo(now));
        assertThat(eventHandle.getExternalOriginationTime(), equalTo(null));
        assertThat(eventHandle.hasAcknowledgementSet(), equalTo(false));
        eventHandle.acquireReference();
        eventHandle.release(true);
    }

    @Test
    void testWithAcknowledgementSet() {
        acknowledgementSet1 = mock(AcknowledgementSet.class);
        acknowledgementSet2 = mock(AcknowledgementSet.class);
        when(acknowledgementSet1.release(any(EventHandle.class), any(Boolean.class))).thenReturn(true);
        when(acknowledgementSet2.release(any(EventHandle.class), any(Boolean.class))).thenReturn(true);
        Instant now = Instant.now();
        AggregateEventHandle eventHandle = new AggregateEventHandle(now);
        assertThat(eventHandle.getInternalOriginationTime(), equalTo(now));
        assertThat(eventHandle.getExternalOriginationTime(), equalTo(null));
        eventHandle.addAcknowledgementSet(acknowledgementSet1);
        // just do duplicate add
        eventHandle.addAcknowledgementSet(acknowledgementSet1);
        assertThat(eventHandle.hasAcknowledgementSet(), equalTo(true));
        eventHandle.addAcknowledgementSet(acknowledgementSet2);
        eventHandle.acquireReference();
        verify(acknowledgementSet1).acquire(eventHandle);
        verify(acknowledgementSet2).acquire(eventHandle);
        eventHandle.release(true);
        verify(acknowledgementSet1).release(eventHandle, true);
        verify(acknowledgementSet2).release(eventHandle, true);
    }

    @Test
    void testWithExternalOriginationTime() {
        Instant now = Instant.now();
        AggregateEventHandle eventHandle = new AggregateEventHandle(now);
        assertThat(eventHandle.hasAcknowledgementSet(), equalTo(false));
        assertThat(eventHandle.getInternalOriginationTime(), equalTo(now));
        assertThat(eventHandle.getExternalOriginationTime(), equalTo(null));
        eventHandle.setExternalOriginationTime(now.minusSeconds(60));
        assertThat(eventHandle.getExternalOriginationTime(), equalTo(now.minusSeconds(60)));
        eventHandle.release(true);
    }

    @Test
    void testWithOnReleaseHandler() {
        Instant now = Instant.now();
        count = 0;
        AggregateEventHandle eventHandle = new AggregateEventHandle(now);
        acknowledgementSet1 = mock(AcknowledgementSet.class);
        acknowledgementSet2 = mock(AcknowledgementSet.class);
        eventHandle.onRelease((handle, result) -> {if (result) count++; });
        eventHandle.addAcknowledgementSet(acknowledgementSet1);
        assertThat(eventHandle.hasAcknowledgementSet(), equalTo(true));
        eventHandle.addAcknowledgementSet(acknowledgementSet2);
        // Simulate weak reference object not available for
        // verification tests to pass 100%
        for (WeakReference<AcknowledgementSet> acknowledgementSetRef: eventHandle.getAcknowledgementSetRefs()) {
            if (acknowledgementSetRef.get() == acknowledgementSet2 ) {
                acknowledgementSetRef.clear();
                break;
            }
        }
        eventHandle.release(true);
        assertThat(count, equalTo(1));
        verify(acknowledgementSet1, times(1)).release(eventHandle, true);
        verify(acknowledgementSet2, times(0)).release(eventHandle, true);

    }

}

