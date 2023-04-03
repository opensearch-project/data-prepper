/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.EventHandle;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import java.time.Duration;

@ExtendWith(MockitoExtension.class)
class DefaultAcknowledgementSetManagerTests {
    private static final Duration TEST_TIMEOUT_MS = Duration.ofMillis(1000);
    DefaultAcknowledgementSetManager acknowledgementSetManager;

    @Mock
    JacksonEvent event1;
    @Mock
    JacksonEvent event2;
    @Mock
    JacksonEvent event3;

    EventHandle eventHandle1;
    EventHandle eventHandle2;
    EventHandle eventHandle3;
    Boolean result;

    @BeforeEach
    void setup() {
        event1 = mock(JacksonEvent.class);
        try {
            doAnswer((i) -> {
                eventHandle1 = (EventHandle)i.getArgument(0);
                return null;
            }).when(event1).setEventHandle(any());
        } catch (Exception e){}
        lenient().when(event1.getEventHandle()).thenReturn(eventHandle1);

        event2 = mock(JacksonEvent.class);
        try {
            doAnswer((i) -> {
                eventHandle2 = (EventHandle)i.getArgument(0);
                return null;
            }).when(event2).setEventHandle(any());
        } catch (Exception e){}
        lenient().when(event2.getEventHandle()).thenReturn(eventHandle2);

        acknowledgementSetManager = createObjectUnderTest();
        AcknowledgementSet acknowledgementSet1 = acknowledgementSetManager.create((flag) -> { result = flag; }, TEST_TIMEOUT_MS);
        acknowledgementSet1.add(event1);
        acknowledgementSet1.add(event2);
    }

    DefaultAcknowledgementSetManager createObjectUnderTest() {
        return new DefaultAcknowledgementSetManager(Duration.ofMillis(TEST_TIMEOUT_MS.toMillis() * 2));
    }

    @Test
    void testBasic() {
        acknowledgementSetManager.releaseEventReference(eventHandle2, true);
        acknowledgementSetManager.releaseEventReference(eventHandle1, true);
        try {
            Thread.sleep(TEST_TIMEOUT_MS.toMillis() * 5);
        } catch (Exception e){}
        assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
        assertThat(result, equalTo(true));
    }

    @Test
    void testExpirations() {
        acknowledgementSetManager.releaseEventReference(eventHandle2, true);
        try {
            Thread.sleep(TEST_TIMEOUT_MS.toMillis() * 5);
        } catch (Exception e){}
        assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
        assertThat(result, equalTo(null));
    }

    @Test
    void testMultipleAcknowledgementSets() {
        event3 = mock(JacksonEvent.class);
        try {
            doAnswer((i) -> {
                eventHandle3 = (EventHandle)i.getArgument(0);
                return null;
            }).when(event3).setEventHandle(any());
        } catch (Exception e){}
        lenient().when(event3.getEventHandle()).thenReturn(eventHandle3);

        AcknowledgementSet acknowledgementSet2 = acknowledgementSetManager.create((flag) -> { result = flag; }, TEST_TIMEOUT_MS);
        acknowledgementSet2.add(event3);

        acknowledgementSetManager.releaseEventReference(eventHandle2, true);
        acknowledgementSetManager.releaseEventReference(eventHandle3, true);
        try {
            Thread.sleep(TEST_TIMEOUT_MS.toMillis() * 5);
        } catch (Exception e){}
        assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
        assertThat(result, equalTo(true));
    }
}
