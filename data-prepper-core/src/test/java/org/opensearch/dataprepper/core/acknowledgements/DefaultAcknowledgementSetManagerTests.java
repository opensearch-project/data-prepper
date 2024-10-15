/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.acknowledgements;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

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
        acknowledgementSetManager = createObjectUnderTest();
        AcknowledgementSet acknowledgementSet1 = acknowledgementSetManager.create((flag) -> { result = flag; }, TEST_TIMEOUT);
        event1 = mock(JacksonEvent.class);
        eventHandle1 = mock(DefaultEventHandle.class);
        lenient().doAnswer(a -> {
            Boolean result = (Boolean)a.getArgument(0);
            acknowledgementSet1.release(eventHandle1, result);
            return null;
        }).when(eventHandle1).release(any(Boolean.class));
        lenient().when(event1.getEventHandle()).thenReturn(eventHandle1);
        pluginMetrics = mock(PluginMetrics.class);

        event2 = mock(JacksonEvent.class);
        eventHandle2 = mock(DefaultEventHandle.class);
        lenient().doAnswer(a -> {
            Boolean result = (Boolean)a.getArgument(0);
            acknowledgementSet1.release(eventHandle2, result);
            return null;
        }).when(eventHandle2).release(any(Boolean.class));
        lenient().when(event2.getEventHandle()).thenReturn(eventHandle2);

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
        eventHandle2.release(true);
        eventHandle1.release(true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
                    assertThat(result, equalTo(true));
                });
    }

    @Test
    void testExpirations() throws InterruptedException {
        eventHandle2.release(true);
        Thread.sleep(TEST_TIMEOUT.multipliedBy(5).toMillis());
        assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(result, equalTo(null));
                });
    }

    @Test
    void testMultipleAcknowledgementSets() {
        AcknowledgementSet acknowledgementSet2 = acknowledgementSetManager.create((flag) -> { result = flag; }, TEST_TIMEOUT);
        event3 = mock(JacksonEvent.class);
        eventHandle3 = mock(DefaultEventHandle.class);
        doAnswer(a -> {
            Boolean result = (Boolean)a.getArgument(0);
            acknowledgementSet2.release(eventHandle3, result);
            return null;
        }).when(eventHandle3).release(any(Boolean.class));
        lenient().when(event3.getEventHandle()).thenReturn(eventHandle3);

        acknowledgementSet2.add(event3);
        lenient().when(eventHandle3.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        acknowledgementSet2.complete();

        eventHandle2.release(true);
        eventHandle3.release(true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                        .untilAsserted(() -> {
                            assertThat(acknowledgementSetManager.getAcknowledgementSetMonitor().getSize(), equalTo(0));
                            assertThat(result, equalTo(true));
                        });
    }

    @Test
    void testWithProgressCheckCallbacks() {
        AcknowledgementSet acknowledgementSet2 = acknowledgementSetManager.create((flag) -> { result = flag; }, Duration.ofMillis(10000));
        eventHandle3 = mock(DefaultEventHandle.class);
        doAnswer(a -> {
            Boolean result = (Boolean)a.getArgument(0);
            acknowledgementSet2.release(eventHandle3, result);
            return null;
        }).when(eventHandle3).release(any(Boolean.class));
        lenient().when(event3.getEventHandle()).thenReturn(eventHandle3);

        eventHandle4 = mock(DefaultEventHandle.class);
        doAnswer(a -> {
            Boolean result = (Boolean)a.getArgument(0);
            acknowledgementSet2.release(eventHandle4, result);
            return null;
        }).when(eventHandle4).release(any(Boolean.class));
        JacksonEvent event4 = mock(JacksonEvent.class);
        lenient().when(event4.getEventHandle()).thenReturn(eventHandle4);

        eventHandle5 = mock(DefaultEventHandle.class);
        doAnswer(a -> {
            Boolean result = (Boolean)a.getArgument(0);
            acknowledgementSet2.release(eventHandle5, result);
            return null;
        }).when(eventHandle5).release(any(Boolean.class));
        JacksonEvent event5 = mock(JacksonEvent.class);
        lenient().when(event5.getEventHandle()).thenReturn(eventHandle5);

        eventHandle6 = mock(DefaultEventHandle.class);
        doAnswer(a -> {
            Boolean result = (Boolean)a.getArgument(0);
            acknowledgementSet2.release(eventHandle6, result);
            return null;
        }).when(eventHandle6).release(any(Boolean.class));
        JacksonEvent event6 = mock(JacksonEvent.class);
        lenient().when(event6.getEventHandle()).thenReturn(eventHandle6);

        acknowledgementSet2.addProgressCheck((progressCheck) -> {currentRatio = progressCheck.getRatio();}, Duration.ofSeconds(1));
        acknowledgementSet2.add(event3);
        acknowledgementSet2.add(event4);
        acknowledgementSet2.add(event5);
        acknowledgementSet2.add(event6);
        lenient().when(eventHandle3.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        lenient().when(eventHandle4.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        lenient().when(eventHandle5.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        lenient().when(eventHandle6.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        acknowledgementSet2.complete();
        eventHandle3.release(true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(currentRatio, equalTo(0.75));
                });
        eventHandle4.release(true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(currentRatio, equalTo(0.5));
                });
        eventHandle5.release(true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(currentRatio, equalTo(0.25));
                });
        eventHandle6.release(true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                assertThat(result, equalTo(true));
                });
        
    }

    @Test
    void testWithProgressCheckCallbacks_AcksExpire() {
        AcknowledgementSet acknowledgementSet2 = acknowledgementSetManager.create((flag) -> { result = flag; }, Duration.ofSeconds(10));
        eventHandle3 = mock(DefaultEventHandle.class);
        doAnswer(a -> {
            Boolean result = (Boolean)a.getArgument(0);
            acknowledgementSet2.release(eventHandle3, result);
            return null;
        }).when(eventHandle3).release(any(Boolean.class));
        lenient().when(event3.getEventHandle()).thenReturn(eventHandle3);

        eventHandle4 = mock(DefaultEventHandle.class);
        doAnswer(a -> {
            Boolean result = (Boolean)a.getArgument(0);
            acknowledgementSet2.release(eventHandle4, result);
            return null;
        }).when(eventHandle4).release(any(Boolean.class));
        JacksonEvent event4 = mock(JacksonEvent.class);
        lenient().when(event4.getEventHandle()).thenReturn(eventHandle4);

        eventHandle5 = mock(DefaultEventHandle.class);
        doAnswer(a -> {
            Boolean result = (Boolean)a.getArgument(0);
            acknowledgementSet2.release(eventHandle5, result);
            return null;
        }).when(eventHandle5).release(any(Boolean.class));
        JacksonEvent event5 = mock(JacksonEvent.class);
        lenient().when(event5.getEventHandle()).thenReturn(eventHandle5);

        eventHandle6 = mock(DefaultEventHandle.class);
        JacksonEvent event6 = mock(JacksonEvent.class);
        lenient().when(event6.getEventHandle()).thenReturn(eventHandle6);

        acknowledgementSet2.addProgressCheck((progressCheck) -> {currentRatio = progressCheck.getRatio();}, Duration.ofSeconds(1));
        acknowledgementSet2.add(event3);
        acknowledgementSet2.add(event4);
        acknowledgementSet2.add(event5);
        acknowledgementSet2.add(event6);
        lenient().when(eventHandle3.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        lenient().when(eventHandle4.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        lenient().when(eventHandle5.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        lenient().when(eventHandle6.getAcknowledgementSet()).thenReturn(acknowledgementSet2);
        acknowledgementSet2.complete();
        eventHandle3.release(true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(currentRatio, equalTo(0.75));
                });
        eventHandle4.release(true);
        await().atMost(TEST_TIMEOUT.multipliedBy(5))
                .untilAsserted(() -> {
                    assertThat(currentRatio, equalTo(0.5));
                });
        eventHandle5.release(true);
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
