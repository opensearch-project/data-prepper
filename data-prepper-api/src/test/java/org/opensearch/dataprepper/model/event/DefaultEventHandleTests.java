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
import static org.mockito.Mockito.doNothing;
import org.mockito.Mock;

import java.time.Instant;

class DefaultEventHandleTests {
    @Mock
    private AcknowledgementSet acknowledgementSet;
    private int count;

    @Test
    void testBasic() {
        Instant now = Instant.now();
        DefaultEventHandle eventHandle = new DefaultEventHandle(now);
        assertThat(eventHandle.getAcknowledgementSet(), equalTo(null));
        assertThat(eventHandle.getInternalOriginationTime(), equalTo(now));
        assertThat(eventHandle.getExternalOriginationTime(), equalTo(null));
        eventHandle.acquireReference();
        assertThat(eventHandle.hasAcknowledgementSet(), equalTo(false));
        eventHandle.release(true);
    }

    @Test
    void testWithAcknowledgementSet() {
        acknowledgementSet = mock(AcknowledgementSet.class);
        when(acknowledgementSet.release(any(EventHandle.class), any(Boolean.class))).thenReturn(true);
        doNothing().when(acknowledgementSet).acquire(any(EventHandle.class));
        Instant now = Instant.now();
        DefaultEventHandle eventHandle = new DefaultEventHandle(now);
        assertThat(eventHandle.getAcknowledgementSet(), equalTo(null));
        assertThat(eventHandle.getInternalOriginationTime(), equalTo(now));
        assertThat(eventHandle.getExternalOriginationTime(), equalTo(null));
        eventHandle.addAcknowledgementSet(acknowledgementSet);
        assertThat(eventHandle.hasAcknowledgementSet(), equalTo(true));
        eventHandle.acquireReference();
        verify(acknowledgementSet).acquire(eventHandle);
        eventHandle.release(true);
        verify(acknowledgementSet).release(eventHandle, true);
    }

    @Test
    void testWithExternalOriginationTime() {
        Instant now = Instant.now();
        DefaultEventHandle eventHandle = new DefaultEventHandle(now);
        assertThat(eventHandle.getAcknowledgementSet(), equalTo(null));
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
        DefaultEventHandle eventHandle = new DefaultEventHandle(now);
        eventHandle.onRelease((handle, result) -> {if (result) count++; });
        eventHandle.release(true);
        assertThat(count, equalTo(1));

    }
  
    @Test
    void testAddEventHandle() {
        Instant now = Instant.now();
        DefaultEventHandle eventHandle = new DefaultEventHandle(now);
        acknowledgementSet = mock(AcknowledgementSet.class);
        eventHandle.addAcknowledgementSet(acknowledgementSet);
        DefaultEventHandle eventHandle2 = new DefaultEventHandle(now);
        doNothing().when(acknowledgementSet).add(any(EventHandle.class));
        eventHandle.addEventHandle(eventHandle2);
        verify(acknowledgementSet).add(eventHandle2);
    }

}
