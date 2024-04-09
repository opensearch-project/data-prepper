/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.awaitility.Awaitility;
import static org.awaitility.Awaitility.await;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class DefaultAcknowledgementSetTests {
    private static final int MAX_THREADS = 3;
    private DefaultAcknowledgementSet defaultAcknowledgementSet;
    @Mock
    private JacksonEvent event;
    @Mock
    private JacksonEvent event2;
    @Mock
    private DefaultEventHandle handle;
    @Mock
    private DefaultEventHandle handle2;

    private double currentRatio;

    private ScheduledExecutorService executor;
    private Boolean acknowledgementSetResult;
    private final Duration TEST_TIMEOUT = Duration.ofMillis(5000);
    private AtomicBoolean callbackInterrupted;
    @Mock
    private DefaultAcknowledgementSetMetrics metrics;
    private int invalidAcquiresCounter;
    private int invalidReleasesCounter;
    
    private void setupMetrics() {
        metrics = mock(DefaultAcknowledgementSetMetrics.class);
        lenient().doAnswer(a -> {
            String metricName = a.getArgument(0);
            if (metricName == DefaultAcknowledgementSetMetrics.INVALID_ACQUIRES_METRIC_NAME) {
                invalidAcquiresCounter++;
            } else if (metricName == DefaultAcknowledgementSetMetrics.INVALID_RELEASES_METRIC_NAME) {
                invalidReleasesCounter++;
            }
            return null;
        }).when(metrics).increment(any(String.class));
    }

    private DefaultAcknowledgementSet createObjectUnderTest() {
        setupMetrics();
        return new DefaultAcknowledgementSet(executor, (flag) ->
                {}, TEST_TIMEOUT, metrics);
    }

    private DefaultAcknowledgementSet createObjectUnderTestWithCallback(Consumer<Boolean> callback) {
        setupMetrics();
        return new DefaultAcknowledgementSet(executor, callback, TEST_TIMEOUT, metrics);
    }

    @BeforeEach
    void setupEvent() {
        executor = Executors.newScheduledThreadPool(MAX_THREADS);

        acknowledgementSetResult = null;
        defaultAcknowledgementSet = createObjectUnderTest();
        callbackInterrupted = new AtomicBoolean(false);

        event = mock(JacksonEvent.class);
        handle = mock(DefaultEventHandle.class);
        lenient().doAnswer(a -> {
            AcknowledgementSet acknowledgementSet = a.getArgument(0);
            lenient().when(handle.getAcknowledgementSet()).thenReturn(acknowledgementSet);
            return null;
        }).when(handle).setAcknowledgementSet(any(AcknowledgementSet.class));
        lenient().when(event.getEventHandle()).thenReturn(handle);
        event2 = mock(JacksonEvent.class);
        lenient().doAnswer(a -> {
            AcknowledgementSet acknowledgementSet = a.getArgument(0);
            lenient().when(handle2.getAcknowledgementSet()).thenReturn(acknowledgementSet);
            return null;
        }).when(handle2).setAcknowledgementSet(any(AcknowledgementSet.class));
        handle2 = mock(DefaultEventHandle.class);
        lenient().when(event2.getEventHandle()).thenReturn(handle2);
    }

    @Test
    void testDefaultAcknowledgementSetBasic() throws Exception {
        defaultAcknowledgementSet.add(event);
        defaultAcknowledgementSet.complete();
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));
    }

    @Test
    void testDefaultAcknowledgementSetMultipleAcquireAndRelease() throws Exception {
        defaultAcknowledgementSet.add(event);
        defaultAcknowledgementSet.complete();
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        defaultAcknowledgementSet.acquire(handle);
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(false));
        defaultAcknowledgementSet.acquire(handle);
        defaultAcknowledgementSet.acquire(handle);
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(false));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(false));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));
    }

    @Test
    void testDefaultAcknowledgementInvalidAcquire() {
        defaultAcknowledgementSet.add(event);
        defaultAcknowledgementSet.complete();
        DefaultAcknowledgementSet secondAcknowledgementSet = createObjectUnderTest();
        defaultAcknowledgementSet.acquire(handle2);
        assertThat(invalidAcquiresCounter, equalTo(1));
    }

    void testDefaultAcknowledgementInvalidRelease() {
        defaultAcknowledgementSet.add(event);
        defaultAcknowledgementSet.complete();
        DefaultAcknowledgementSet secondAcknowledgementSet = createObjectUnderTest();
        assertThat(defaultAcknowledgementSet.release(handle2, true), equalTo(false));
    }

    @Test
    void testDefaultAcknowledgementDuplicateReleaseError() throws Exception {
        defaultAcknowledgementSet.add(event);
        defaultAcknowledgementSet.complete();
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(false));
    }

    @Test
    void testDefaultAcknowledgementSetWithCustomCallback() throws Exception {
        defaultAcknowledgementSet = createObjectUnderTestWithCallback(
            (flag) -> {
                acknowledgementSetResult = flag;
            }        
        );
        defaultAcknowledgementSet.add(event);
        defaultAcknowledgementSet.complete();
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));

        Awaitility.waitAtMost(Duration.ofSeconds(10))
                .pollDelay(Duration.ofMillis(500))
                .until(() -> defaultAcknowledgementSet.isDone());
        assertThat(acknowledgementSetResult, equalTo(true));
    }

    @Test
    void testDefaultAcknowledgementSetNegativeAcknowledgements() throws Exception {
        defaultAcknowledgementSet = createObjectUnderTestWithCallback(
            (flag) -> {
                acknowledgementSetResult = flag;
            }        
        );
        defaultAcknowledgementSet.add(event);
        defaultAcknowledgementSet.complete();
        assertThat(handle, not(equalTo(null)));
        lenient().doAnswer(a -> {
            AcknowledgementSet acknowledgementSet = a.getArgument(0);
            lenient().when(handle.getAcknowledgementSet()).thenReturn(acknowledgementSet);
            return null;
        }).when(handle).setAcknowledgementSet(any(AcknowledgementSet.class));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        defaultAcknowledgementSet.acquire(handle);
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(false));
        defaultAcknowledgementSet.acquire(handle);
        defaultAcknowledgementSet.acquire(handle);
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(false));
        assertThat(defaultAcknowledgementSet.release(handle, false), equalTo(false));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));
        Awaitility.waitAtMost(Duration.ofSeconds(10))
                .pollDelay(Duration.ofMillis(500))
                .until(() -> defaultAcknowledgementSet.isDone());
        assertThat(acknowledgementSetResult, equalTo(false));
    }

    @Test
    void testDefaultAcknowledgementSetExpirations() throws Exception {
        defaultAcknowledgementSet = createObjectUnderTestWithCallback(
            (flag) -> {
                try {
                    Thread.sleep(3 * TEST_TIMEOUT.toMillis());
                    acknowledgementSetResult = flag;
                } catch (Exception e) {
                    callbackInterrupted.set(true);
                }
            }        
        );
        defaultAcknowledgementSet.add(event);
        defaultAcknowledgementSet.complete();
        lenient().doAnswer(a -> {
            AcknowledgementSet acknowledgementSet = a.getArgument(0);
            lenient().when(handle.getAcknowledgementSet()).thenReturn(acknowledgementSet);
            return null;
        }).when(handle).setAcknowledgementSet(any(AcknowledgementSet.class));
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));
        Awaitility.waitAtMost(Duration.ofSeconds(10))
                .pollDelay(Duration.ofMillis(500))
                .until(() -> defaultAcknowledgementSet.isDone());
        assertThat(acknowledgementSetResult, equalTo(null));
        Awaitility.waitAtMost(Duration.ofSeconds(20))
                .pollDelay(Duration.ofMillis(500))
                .until(() -> callbackInterrupted.get());
        assertThat(callbackInterrupted.get(), equalTo(true));
    }

    @Test
    void testDefaultAcknowledgementSetWithProgressCheck() throws Exception {
        defaultAcknowledgementSet = createObjectUnderTestWithCallback(
            (flag) -> {
                acknowledgementSetResult = flag;
            }        
        );
        defaultAcknowledgementSet.addProgressCheck(
            (progressCheck) -> {
                currentRatio = progressCheck.getRatio();
            },
            Duration.ofSeconds(1)
        );
        defaultAcknowledgementSet.add(event);
        defaultAcknowledgementSet.add(event2);
        defaultAcknowledgementSet.complete();
        lenient().doAnswer(a -> {
            AcknowledgementSet acknowledgementSet = a.getArgument(0);
            lenient().when(handle.getAcknowledgementSet()).thenReturn(acknowledgementSet);
            return null;
        }).when(handle).setAcknowledgementSet(any(AcknowledgementSet.class));
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(currentRatio, equalTo(1.0));
                });
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(false));
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(currentRatio, equalTo(0.5));
                });
        assertThat(defaultAcknowledgementSet.release(handle2, true), equalTo(true));
        Awaitility.waitAtMost(Duration.ofSeconds(10))
                .pollDelay(Duration.ofMillis(500))
                .until(() -> defaultAcknowledgementSet.isDone());
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(acknowledgementSetResult, equalTo(true));
                });
    }
}
