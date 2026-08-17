/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.acknowledgements;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Optional;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AcknowledgementSetMonitorThreadTest {
    @Mock
    private AcknowledgementSetMonitor acknowledgementSetMonitor;
    private Duration delayTime;

    @BeforeEach
    void setUp() {
        delayTime = Duration.ofMillis(10);
    }

    private AcknowledgementSetMonitorThread createObjectUnderTest() {
        return new AcknowledgementSetMonitorThread(acknowledgementSetMonitor, delayTime);
    }

    @Test
    void run_will_call_monitor_run() {
        final AcknowledgementSetMonitorThread objectUnderTest = createObjectUnderTest();

        objectUnderTest.start();
        await().atMost(delayTime.plusMillis(500))
                .untilAsserted(() -> {
                    verify(acknowledgementSetMonitor, atLeastOnce()).run();
                });

        verify(acknowledgementSetMonitor, atLeastOnce()).run();

        objectUnderTest.stop();
    }

    @Test
    void stop_when_thread_is_sleeping_interrupts_and_stops_the_thread_promptly() {
        delayTime = Duration.ofMinutes(5);
        final AcknowledgementSetMonitorThread objectUnderTest = createObjectUnderTest();

        objectUnderTest.start();
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    verify(acknowledgementSetMonitor, atLeastOnce()).run();
                });

        final Optional<Thread> monitorThread = findMonitorThread();
        assertThat(monitorThread.isPresent(), equalTo(true));

        final long stopStartTimeNanos = System.nanoTime();
        objectUnderTest.stop();
        final Duration stopDuration = Duration.ofNanos(System.nanoTime() - stopStartTimeNanos);

        assertThat(stopDuration, lessThan(Duration.ofSeconds(2)));
        assertThat(monitorThread.get().isAlive(), equalTo(false));
    }

    @Test
    void run_when_thread_is_interrupted_exits_the_loop() {
        delayTime = Duration.ofMinutes(5);
        final AcknowledgementSetMonitorThread objectUnderTest = createObjectUnderTest();

        objectUnderTest.start();
        await().atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    verify(acknowledgementSetMonitor, atLeastOnce()).run();
                });

        final Optional<Thread> monitorThread = findMonitorThread();
        assertThat(monitorThread.isPresent(), equalTo(true));
        monitorThread.get().interrupt();

        await().atMost(Duration.ofSeconds(2))
                .until(() -> !monitorThread.get().isAlive());
    }

    @Test
    void stop_when_thread_was_never_started_returns_promptly() {
        final AcknowledgementSetMonitorThread objectUnderTest = createObjectUnderTest();

        final long stopStartTimeNanos = System.nanoTime();
        objectUnderTest.stop();
        final Duration stopDuration = Duration.ofNanos(System.nanoTime() - stopStartTimeNanos);

        assertThat(stopDuration, lessThan(Duration.ofSeconds(2)));
    }

    private Optional<Thread> findMonitorThread() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> "acknowledgement-monitor".equals(thread.getName()))
                .filter(Thread::isAlive)
                .findFirst();
    }
}