/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class DefaultAcknowledgementSetTests {
    private static final int MAX_THREADS = 3;
    private DefaultAcknowledgementSet defaultAcknowledgementSet;
    @Mock
    private JacksonEvent event;
    private DefaultEventHandle handle;

    private ExecutorService executor;
    private Boolean acknowledgementSetResult;
    private final Duration TEST_TIMEOUT = Duration.ofMillis(5000);
    private AtomicBoolean callbackInterrupted;
    
    private DefaultAcknowledgementSet createObjectUnderTest() {
        return new DefaultAcknowledgementSet(executor, (flag) ->
                {}, TEST_TIMEOUT);
    }

    private DefaultAcknowledgementSet createObjectUnderTestWithCallback(Consumer<Boolean> callback) {
        return new DefaultAcknowledgementSet(executor, callback, TEST_TIMEOUT);
    }

    @BeforeEach
    void setupEvent() {
        executor = Executors.newFixedThreadPool(MAX_THREADS);

        acknowledgementSetResult = null;
        defaultAcknowledgementSet = createObjectUnderTest();
        callbackInterrupted = new AtomicBoolean(false);

        event = mock(JacksonEvent.class);
        
        doAnswer((i) -> {
            handle = i.getArgument(0);
            return null;
        }).when(event).setEventHandle(any());
        lenient().when(event.getEventHandle()).thenReturn(handle);
    }

    @Test
    void testDefaultAcknowledgementSetBasic() throws Exception {
        defaultAcknowledgementSet.add(event);
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));
    }

    @Test
    void testDefaultAcknowledgementSetMultipleAcquireAndRelease() throws Exception {
        defaultAcknowledgementSet.add(event);
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
        DefaultAcknowledgementSet secondAcknowledgementSet = createObjectUnderTest();
        DefaultEventHandle handle2 = new DefaultEventHandle(secondAcknowledgementSet);
        defaultAcknowledgementSet.acquire(handle2);
        assertThat(defaultAcknowledgementSet.getNumInvalidAcquires(), equalTo(1));
    }

    @Test
    void testDefaultAcknowledgementInvalidRelease() {
        defaultAcknowledgementSet.add(event);
        DefaultAcknowledgementSet secondAcknowledgementSet = createObjectUnderTest();
        DefaultEventHandle handle2 = new DefaultEventHandle(secondAcknowledgementSet);
        assertThat(defaultAcknowledgementSet.release(handle2, true), equalTo(false));
        assertThat(defaultAcknowledgementSet.getNumInvalidReleases(), equalTo(1));
    }

    @Test
    void testDefaultAcknowledgementDuplicateReleaseError() throws Exception {
        defaultAcknowledgementSet.add(event);
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(false));
        assertThat(defaultAcknowledgementSet.getNumInvalidReleases(), equalTo(1));
    }

    @Test
    void testDefaultAcknowledgementSetWithCustomCallback() throws Exception {
        defaultAcknowledgementSet = createObjectUnderTestWithCallback(
            (flag) -> {
                acknowledgementSetResult = flag;
            }        
        );
        defaultAcknowledgementSet.add(event);
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
        assertThat(handle, not(equalTo(null)));
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
}
