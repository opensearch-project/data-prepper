/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mock;

class DefaultEventHandleTests {
    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Test
    void testBasic() {
        acknowledgementSet = mock(AcknowledgementSet.class);
        when(acknowledgementSet.release(any(EventHandle.class), any(Boolean.class))).thenReturn(true);
        DefaultEventHandle eventHandle = new DefaultEventHandle(acknowledgementSet);
        assertThat(eventHandle.getAcknowledgementSet(), equalTo(acknowledgementSet));
        eventHandle.release(true);
        verify(acknowledgementSet).release(eventHandle, true);
    }
}
