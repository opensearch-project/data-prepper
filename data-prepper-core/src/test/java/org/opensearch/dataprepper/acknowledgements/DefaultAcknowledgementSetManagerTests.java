/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;

@ExtendWith(MockitoExtension.class)
class DefaultAcknowledgementSetManagerTests {
    private static final Duration TEST_TIMEOUT = Duration.ofMillis(400);
    private DefaultAcknowledgementSetManager acknowledgementSetManager;
    private ScheduledExecutorService callbackExecutor;

    @Mock
    JacksonEvent event1;
    @Mock
    JacksonEvent event2;
    @Mock
    JacksonEvent event3;

    private PluginMetrics pluginMetrics;
    private DefaultEventHandle eventHandle1;
    private DefaultEventHandle eventHandle2;
    private DefaultEventHandle eventHandle3;
    private DefaultEventHandle eventHandle4;
    private DefaultEventHandle eventHandle5;
    private DefaultEventHandle eventHandle6;
    private Boolean result;
    private double currentRatio;

    @BeforeEach
    void setup() {
        currentRatio = 0;
        callbackExecutor = Executors.newScheduledThreadPool(2);
        event1 = mock(JacksonEvent.class);
        eventHandle1 = mock(DefaultEventHandle.class);
        lenient().when(event1.getEventHandle()).thenReturn(eventHandle1);
        pluginMetrics = mock(PluginMetrics.class);

        event2 = mock(JacksonEvent.class);
        eventHandle2 = mock(DefaultEventHandle.class);
        lenient().when(event2.getEventHandle()).thenReturn(eventHandle2);

        acknowledgementSetManager = createObjectUnderTest();
        AcknowledgementSet acknowledgementSet1 = acknowledgementSetManager.create((flag) -> { result = flag; }, TEST_TIMEOUT);
        acknowledgementSet1.add(event1);
        acknowledgementSet1.add(event2);
        lenient().when(eventHandle1.getAcknowledgementSet()).thenReturn(acknowledgementSet1);
        lenient().when(eventHandle2.getAcknowledgementSet()).thenReturn(acknowledgementSet1);
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
    }

    @Test
    void testExpirations() throws InterruptedException {
        acknowledgementSetManager.releaseEventReference(eventHandle2, true);
        Thread.sleep(TEST_TIMEOUT.multipliedBy(5).toMillis());
        assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(result, equalTo(null));
                });
    }

    @Test
    void testMultipleAcknowledgementSets() {
        event3 = mock(JacksonEvent.class);
        eventHandle3 = mock(DefaultEventHandle.class);
        lenient().when(event3.getEventHandle()).thenReturn(eventHandle3);

        AcknowledgementSet acknowledgementSet2 = acknowledgementSetManager.create((flag) -> { result = flag; }, TEST_TIMEOUT);
        acknowledgementSet2.add(event3);
        lenient().when(eventHandle3.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        acknowledgementSet2.complete();

        acknowledgementSetManager.releaseEventReference(eventHandle2, true);
        acknowledgementSetManager.releaseEventReference(eventHandle3, true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                        .untilAsserted(() -> {
                            assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
                            assertThat(result, equalTo(true));
                        });
    }

    @Test
    void testWithProgressCheckCallbacks() {
        eventHandle3 = mock(DefaultEventHandle.class);
        lenient().when(event3.getEventHandle()).thenReturn(eventHandle3);

        eventHandle4 = mock(DefaultEventHandle.class);
        JacksonEvent event4 = mock(JacksonEvent.class);
        lenient().when(event4.getEventHandle()).thenReturn(eventHandle4);

        eventHandle5 = mock(DefaultEventHandle.class);
        JacksonEvent event5 = mock(JacksonEvent.class);
        lenient().when(event5.getEventHandle()).thenReturn(eventHandle5);

        eventHandle6 = mock(DefaultEventHandle.class);
        JacksonEvent event6 = mock(JacksonEvent.class);
        lenient().when(event6.getEventHandle()).thenReturn(eventHandle6);

        AcknowledgementSet acknowledgementSet2 = acknowledgementSetManager.create((flag) -> { result = flag; }, Duration.ofMillis(10000));
        acknowledgementSet2.addProgressCheck((ratio) -> {currentRatio = ratio;}, Duration.ofSeconds(1));
        acknowledgementSet2.add(event3);
        acknowledgementSet2.add(event4);
        acknowledgementSet2.add(event5);
        acknowledgementSet2.add(event6);
        lenient().when(eventHandle3.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        lenient().when(eventHandle4.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        lenient().when(eventHandle5.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        lenient().when(eventHandle6.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        acknowledgementSet2.complete();
        acknowledgementSetManager.releaseEventReference(eventHandle3, true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(currentRatio, equalTo(0.75));
                });
        acknowledgementSetManager.releaseEventReference(eventHandle4, true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(currentRatio, equalTo(0.5));
                });
        acknowledgementSetManager.releaseEventReference(eventHandle5, true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(currentRatio, equalTo(0.25));
                });
        acknowledgementSetManager.releaseEventReference(eventHandle6, true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                assertThat(result, equalTo(true));
                });
        
    }

    @Test
    void testWithProgressCheckCallbacks_AcksExpire() {
        eventHandle3 = mock(DefaultEventHandle.class);
        lenient().when(event3.getEventHandle()).thenReturn(eventHandle3);

        eventHandle4 = mock(DefaultEventHandle.class);
        JacksonEvent event4 = mock(JacksonEvent.class);
        lenient().when(event4.getEventHandle()).thenReturn(eventHandle4);

        eventHandle5 = mock(DefaultEventHandle.class);
        JacksonEvent event5 = mock(JacksonEvent.class);
        lenient().when(event5.getEventHandle()).thenReturn(eventHandle5);

        eventHandle6 = mock(DefaultEventHandle.class);
        JacksonEvent event6 = mock(JacksonEvent.class);
        lenient().when(event6.getEventHandle()).thenReturn(eventHandle6);

        AcknowledgementSet acknowledgementSet2 = acknowledgementSetManager.create((flag) -> { result = flag; }, Duration.ofSeconds(10));
        acknowledgementSet2.addProgressCheck((ratio) -> {currentRatio = ratio;}, Duration.ofSeconds(1));
        acknowledgementSet2.add(event3);
        acknowledgementSet2.add(event4);
        acknowledgementSet2.add(event5);
        acknowledgementSet2.add(event6);
        lenient().when(eventHandle3.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        lenient().when(eventHandle4.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        lenient().when(eventHandle5.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        lenient().when(eventHandle6.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        acknowledgementSet2.complete();
        acknowledgementSetManager.releaseEventReference(eventHandle3, true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(currentRatio, equalTo(0.75));
                });
        acknowledgementSetManager.releaseEventReference(eventHandle4, true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(currentRatio, equalTo(0.5));
                });
        acknowledgementSetManager.releaseEventReference(eventHandle5, true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(currentRatio, equalTo(0.25));
                });
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                assertThat(result, equalTo(null));
                });
        
    }

}
