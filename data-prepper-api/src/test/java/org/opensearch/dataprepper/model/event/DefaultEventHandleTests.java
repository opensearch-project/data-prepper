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
import org.mockito.Mock;

import java.time.Instant;

class DefaultEventHandleTests {
    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Test
    void testBasic() {
        Instant now = Instant.now();
        DefaultEventHandle eventHandle = new DefaultEventHandle(now);
        assertThat(eventHandle.getAcknowledgementSet(), equalTo(null));
        assertThat(eventHandle.getInternalOriginationTime(), equalTo(now));
        assertThat(eventHandle.getExternalOriginationTime(), equalTo(null));
        eventHandle.release(true);
    }

    @Test
    void testWithAcknowledgementSet() {
        acknowledgementSet = mock(AcknowledgementSet.class);
        when(acknowledgementSet.release(any(EventHandle.class), any(Boolean.class))).thenReturn(true);
        Instant now = Instant.now();
        DefaultEventHandle eventHandle = new DefaultEventHandle(now);
        assertThat(eventHandle.getAcknowledgementSet(), equalTo(null));
        assertThat(eventHandle.getInternalOriginationTime(), equalTo(now));
        assertThat(eventHandle.getExternalOriginationTime(), equalTo(null));
        eventHandle.setAcknowledgementSet(acknowledgementSet);
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
}
