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

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ExtendWith(MockitoExtension.class)
class DefaultAcknowledgementSetManagerTests {
    private static final Duration TEST_TIMEOUT = Duration.ofMillis(400);
    DefaultAcknowledgementSetManager acknowledgementSetManager;
    private ExecutorService callbackExecutor;

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
        callbackExecutor = Executors.newFixedThreadPool(2);
        event1 = mock(JacksonEvent.class);
        doAnswer((i) -> {
            eventHandle1 = i.getArgument(0);
            return null;
        }).when(event1).setEventHandle(any());
        lenient().when(event1.getEventHandle()).thenReturn(eventHandle1);

        event2 = mock(JacksonEvent.class);
        doAnswer((i) -> {
            eventHandle2 = i.getArgument(0);
            return null;
        }).when(event2).setEventHandle(any());
        lenient().when(event2.getEventHandle()).thenReturn(eventHandle2);

        acknowledgementSetManager = createObjectUnderTest();
        AcknowledgementSet acknowledgementSet1 = acknowledgementSetManager.create((flag) -> { result = flag; }, TEST_TIMEOUT);
        acknowledgementSet1.add(event1);
        acknowledgementSet1.add(event2);
        acknowledgementSet1.complete();
    }

    DefaultAcknowledgementSetManager createObjectUnderTest() {
        return new DefaultAcknowledgementSetManager(callbackExecutor, Duration.ofMillis(TEST_TIMEOUT.toMillis() * 2));
    }

    @Test
    void testBasic() {
        acknowledgementSetManager.releaseEventReference(eventHandle2, true);
        acknowledgementSetManager.releaseEventReference(eventHandle1, true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
                    assertThat(result, equalTo(true));
                });
        assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
        assertThat(result, equalTo(true));
    }

    @Test
    void testExpirations() throws InterruptedException {
        acknowledgementSetManager.releaseEventReference(eventHandle2, true);
        Thread.sleep(TEST_TIMEOUT.multipliedBy(5).toMillis());
        assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
        assertThat(result, equalTo(null));
    }

    @Test
    void testMultipleAcknowledgementSets() {
        event3 = mock(JacksonEvent.class);
        doAnswer((i) -> {
            eventHandle3 = i.getArgument(0);
            return null;
        }).when(event3).setEventHandle(any());
        lenient().when(event3.getEventHandle()).thenReturn(eventHandle3);

        AcknowledgementSet acknowledgementSet2 = acknowledgementSetManager.create((flag) -> { result = flag; }, TEST_TIMEOUT);
        acknowledgementSet2.add(event3);
        acknowledgementSet2.complete();

        acknowledgementSetManager.releaseEventReference(eventHandle2, true);
        acknowledgementSetManager.releaseEventReference(eventHandle3, true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                        .untilAsserted(() -> {
                            assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
                            assertThat(result, equalTo(true));
                        });
        assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
        assertThat(result, equalTo(true));
    }
}
