/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.event.DefaultEventBuilder;
import org.opensearch.dataprepper.event.DefaultEventFactory;
import org.opensearch.dataprepper.event.DefaultEventHandle;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.commons.lang3.RandomStringUtils;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import java.util.Map;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Duration;

class DefaultAcknowledgementSetTests {
    private static final int MAX_THREADS = 3;
    private DefaultAcknowledgementSet defaultAcknowledgementSet;
    private JacksonEvent event;
    private ScheduledExecutorService executor;
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
        executor = Executors.newScheduledThreadPool(MAX_THREADS);
        DefaultEventFactory eventFactory = new DefaultEventFactory();
        String testKey = RandomStringUtils.randomAlphabetic(5);
        String testValue = RandomStringUtils.randomAlphabetic(10);
        Map<String, Object> data = Map.of(testKey, testValue);
        Map<String, Object> attributes = Collections.emptyMap();
        final DefaultEventBuilder eventBuilder = (DefaultEventBuilder) eventFactory.eventBuilder(DefaultEventBuilder.class).withEventMetadataAttributes(attributes).withData(data);
        event = (JacksonEvent) eventBuilder.build();
        acknowledgementSetResult = null;
        defaultAcknowledgementSet = createObjectUnderTest();
        callbackInterrupted = new AtomicBoolean(false);
    }

    @Test
    void testDefaultAcknowledgementSetBasic() throws Exception {
        defaultAcknowledgementSet.add(event);
        DefaultEventHandle handle = (DefaultEventHandle) event.getEventHandle();
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));
    }

    @Test
    void testDefaultAcknowledgementSetMultipleAcquireAndRelease() throws Exception {
        defaultAcknowledgementSet.add(event);
        DefaultEventHandle handle = (DefaultEventHandle) event.getEventHandle();
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
        DefaultEventHandle handle = new DefaultEventHandle(secondAcknowledgementSet);
        defaultAcknowledgementSet.acquire(handle);
        assertThat(defaultAcknowledgementSet.getNumInvalidAcquires(), equalTo(1));
    }

    @Test
    void testDefaultAcknowledgementInvalidRelease() {
        defaultAcknowledgementSet.add(event);
        DefaultAcknowledgementSet secondAcknowledgementSet = createObjectUnderTest();
        DefaultEventHandle handle = new DefaultEventHandle(secondAcknowledgementSet);
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(false));
        assertThat(defaultAcknowledgementSet.getNumInvalidReleases(), equalTo(1));
    }

    @Test
    void testDefaultAcknowledgementDuplicateReleaseError() throws Exception {
        defaultAcknowledgementSet.add(event);
        DefaultEventHandle handle = (DefaultEventHandle) event.getEventHandle();
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
        DefaultEventHandle handle = (DefaultEventHandle) event.getEventHandle();
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));

        while (!defaultAcknowledgementSet.isDone()) {
            Thread.sleep(1000);
        }
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
        DefaultEventHandle handle = (DefaultEventHandle) event.getEventHandle();
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        defaultAcknowledgementSet.acquire(handle);
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(false));
        defaultAcknowledgementSet.acquire(handle);
        defaultAcknowledgementSet.acquire(handle);
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(false));
        assertThat(defaultAcknowledgementSet.release(handle, false), equalTo(false));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));
        while (!defaultAcknowledgementSet.isDone()) {
            Thread.sleep(1000);
        }
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
        DefaultEventHandle handle = (DefaultEventHandle) event.getEventHandle();
        assertThat(handle, not(equalTo(null)));
        assertThat(handle.getAcknowledgementSet(), equalTo(defaultAcknowledgementSet));
        assertThat(defaultAcknowledgementSet.release(handle, true), equalTo(true));
        while (!defaultAcknowledgementSet.isDone()) {
            Thread.sleep(1000);
        }
        assertThat(acknowledgementSetResult, equalTo(null));
        final int MAX_TRIES = 10;
        int numTries = 0;
        // Try few times
        while (numTries++ < MAX_TRIES && !callbackInterrupted.get()) {
            try {
                Thread.sleep(1000);
            } catch (Exception e){}
        }
        assertThat(callbackInterrupted.get(), equalTo(true));
    }
}
