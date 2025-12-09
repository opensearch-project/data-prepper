/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.acknowledgements;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
            if (DefaultAcknowledgementSetMetrics.INVALID_ACQUIRES_METRIC_NAME.equals(metricName)) {
                invalidAcquiresCounter++;
            } else if (DefaultAcknowledgementSetMetrics.INVALID_RELEASES_METRIC_NAME.equals(metricName)) {
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
        }).when(handle).addAcknowledgementSet(any(AcknowledgementSet.class));
        lenient().when(event.getEventHandle()).thenReturn(handle);
        event2 = mock(JacksonEvent.class);
        lenient().doAnswer(a -> {
            AcknowledgementSet acknowledgementSet = a.getArgument(0);
            lenient().when(handle2.getAcknowledgementSet()).thenReturn(acknowledgementSet);
            return null;
        }).when(handle2).addAcknowledgementSet(any(AcknowledgementSet.class));
        handle2 = mock(DefaultEventHandle.class);
        lenient().when(event2.getEventHandle()).thenReturn(handle2);
    }

    @Test
    void testDefaultAcknowledgementSetBasic() {
        defaultAcknowledgementSet.add(event);
        defaultAcknowledgementSet.complete();
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));
    }

    @Test
    void testDefaultAcknowledgementSetMultipleAcquireAndRelease() {
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
        defaultAcknowledgementSet.add(event.getEventHandle());
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
    void testDefaultAcknowledgementDuplicateReleaseError() {
        defaultAcknowledgementSet.add(event);
        defaultAcknowledgementSet.complete();
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(false));
    }

    @Test
    void testDefaultAcknowledgementSetWithCustomCallback() {
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
    void testDefaultAcknowledgementSetNegativeAcknowledgements() {
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
        }).when(handle).addAcknowledgementSet(any(AcknowledgementSet.class));
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
    void testDefaultAcknowledgementSetExpirations() {
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
        }).when(handle).addAcknowledgementSet(any(AcknowledgementSet.class));
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
    void testDefaultAcknowledgementSetWithProgressCheck() {
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
        defaultAcknowledgementSet.add(event2.getEventHandle());
        defaultAcknowledgementSet.complete();
        lenient().doAnswer(a -> {
            AcknowledgementSet acknowledgementSet = a.getArgument(0);
            lenient().when(handle.getAcknowledgementSet()).thenReturn(acknowledgementSet);
            return null;
        }).when(handle).addAcknowledgementSet(any(AcknowledgementSet.class));
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

    @Test
    void increase_expiry_increase_acknowledgment_set_expiry_time() {
        final AcknowledgementSet objectUnderTest = createObjectUnderTest();
        final Instant nowPlusEightSeconds = Instant.now().plusSeconds(8);

        objectUnderTest.increaseExpiry(Duration.ofSeconds(10));

        assertThat(objectUnderTest.getExpirationTime().isAfter(nowPlusEightSeconds), equalTo(true));
    }

    @Test
    void shutdown_cancels_progress_check_and_callback_future() throws NoSuchFieldException, IllegalAccessException {
        final AcknowledgementSet objectUnderTest = createObjectUnderTest();

        final ScheduledFuture<?> progressCheck = mock(ScheduledFuture.class);
        when(progressCheck.cancel(true)).thenReturn(true);

        final Future<?> callbackFuture = mock(Future.class);
        when(callbackFuture.cancel(false)).thenReturn(true);

        ReflectivelySetField.setField(DefaultAcknowledgementSet.class, objectUnderTest, "progressCheckFuture", progressCheck);
        ReflectivelySetField.setField(DefaultAcknowledgementSet.class, objectUnderTest, "callbackFuture", callbackFuture);

        objectUnderTest.cancel();

        verify(callbackFuture).cancel(false);
        verify(progressCheck).cancel(true);
    }

    @Test
    void testCallbackInvokedWithFalseOnTimeout() throws InterruptedException {
        // Verify callback is invoked with false when acknowledgement set times out
        final AtomicBoolean callbackInvoked = new AtomicBoolean(false);
        final AtomicBoolean callbackResult = new AtomicBoolean(true);

        final Duration shortTimeout = Duration.ofMillis(100);
        final DefaultAcknowledgementSet acknowledgementSet =
                new DefaultAcknowledgementSet(executor, (result) -> {
                    callbackInvoked.set(true);
                    callbackResult.set(result);
                }, shortTimeout, metrics);

        // Wait for timeout to occur
        Thread.sleep(150);

        // Trigger timeout check and verify it's done
        assertThat(acknowledgementSet.isDone(), equalTo(true));

        // Wait for callback to execute
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    assertThat(callbackInvoked.get(), equalTo(true));
                    assertThat(callbackResult.get(), equalTo(false));
                });
    }

    @Test
    void testCallbackInvokedOnlyOnceWhenTimeoutOccurs() throws InterruptedException {
        // Verify callback is not invoked twice if isDone() is called multiple times after timeout
        final AtomicBoolean callbackInvoked = new AtomicBoolean(false);
        final AtomicBoolean callbackResult = new AtomicBoolean(true);

        final Duration shortTimeout = Duration.ofMillis(100);
        final DefaultAcknowledgementSet acknowledgementSet =
                new DefaultAcknowledgementSet(executor, (result) -> {
                    callbackInvoked.set(true);
                    callbackResult.set(result);
                }, shortTimeout, metrics);

        // Wait for timeout
        Thread.sleep(150);

        // Call isDone multiple times and verify it returns true
        assertThat(acknowledgementSet.isDone(), equalTo(true));
        assertThat(acknowledgementSet.isDone(), equalTo(true));
        assertThat(acknowledgementSet.isDone(), equalTo(true));

        // Wait for any callbacks to execute
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    assertThat(callbackInvoked.get(), equalTo(true));
                    assertThat(callbackResult.get(), equalTo(false));
                });
    }

    @Test
    void testCallbackNotInvokedOnTimeoutIfAlreadyCompleted() throws InterruptedException {
        // Verify that if acknowledgement completes normally before timeout,
        // timeout doesn't invoke callback again
        final AtomicBoolean callbackInvoked = new AtomicBoolean(false);
        final AtomicBoolean lastCallbackResult = new AtomicBoolean(false);

        final Duration timeout = Duration.ofSeconds(5);
        final DefaultAcknowledgementSet acknowledgementSet =
                new DefaultAcknowledgementSet(executor, (result) -> {
                    callbackInvoked.set(true);
                    lastCallbackResult.set(result);
                }, timeout, metrics);

        // Add and release an event (normal completion)
        acknowledgementSet.add(event);
        acknowledgementSet.complete();
        assertThat(acknowledgementSet.release(handle, true), equalTo(true));

        // Wait for callback
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    assertThat(callbackInvoked.get(), equalTo(true));
                    assertThat(lastCallbackResult.get(), equalTo(true));
                });

        // Reset flag to detect if callback is invoked again
        callbackInvoked.set(false);

        // Manually trigger timeout check (simulating late timeout check) and verify it's done
        assertThat(acknowledgementSet.isDone(), equalTo(true));
        Thread.sleep(100);

        // Verify callback was not invoked again
        assertThat(callbackInvoked.get(), equalTo(false));
    }

    @Test
    void testExistingPositiveAcknowledgementBehaviorUnchanged() throws InterruptedException {
        // Verify normal positive acknowledgement flow still works as before
        final AtomicBoolean callbackInvoked = new AtomicBoolean(false);
        final AtomicBoolean callbackResult = new AtomicBoolean(false);

        final DefaultAcknowledgementSet acknowledgementSet =
                new DefaultAcknowledgementSet(executor, (result) -> {
                    callbackInvoked.set(true);
                    callbackResult.set(result);
                }, Duration.ofMinutes(5), metrics);

        acknowledgementSet.add(event);
        acknowledgementSet.complete();
        assertThat(acknowledgementSet.release(handle, true), equalTo(true));

        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    assertThat(callbackInvoked.get(), equalTo(true));
                    assertThat(callbackResult.get(), equalTo(true));
                });
    }

    @Test
    void testExistingNegativeAcknowledgementBehaviorUnchanged() throws InterruptedException {
        // Verify normal negative acknowledgement flow still works as before
        final AtomicBoolean callbackInvoked = new AtomicBoolean(false);
        final AtomicBoolean callbackResult = new AtomicBoolean(true);

        final DefaultAcknowledgementSet acknowledgementSet =
                new DefaultAcknowledgementSet(executor, (result) -> {
                    callbackInvoked.set(true);
                    callbackResult.set(result);
                }, Duration.ofMinutes(5), metrics);

        acknowledgementSet.add(event);
        acknowledgementSet.complete();
        assertThat(acknowledgementSet.release(handle, false), equalTo(true));  // Negative acknowledgement

        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    assertThat(callbackInvoked.get(), equalTo(true));
                    assertThat(callbackResult.get(), equalTo(false));
                });
    }
}
