/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.time.Duration;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for Office365MetricsRecorder.
 * 
 * Tests the recording of Office 365 specific metrics including:
 * - StartSubscription success/failure tracking
 * - StartSubscription latency measurement with multiple overloads
 * - StartSubscription API call counting
 * - Various metric increment scenarios and edge cases
 */
@ExtendWith(MockitoExtension.class)
class Office365MetricsRecorderTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter startSubscriptionSuccessCounter;

    @Mock
    private Counter startSubscriptionFailureCounter;

    @Mock
    private Timer startSubscriptionLatencyTimer;

    @Mock
    private Counter startSubscriptionCallsCounter;

    private Office365MetricsRecorder office365MetricsRecorder;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter("startSubscriptionRequestsSuccess")).thenReturn(startSubscriptionSuccessCounter);
        when(pluginMetrics.counter("startSubscriptionRequestsFailed")).thenReturn(startSubscriptionFailureCounter);
        when(pluginMetrics.timer("startSubscriptionRequestLatency")).thenReturn(startSubscriptionLatencyTimer);
        when(pluginMetrics.counter("startSubscriptionApiCalls")).thenReturn(startSubscriptionCallsCounter);

        office365MetricsRecorder = new Office365MetricsRecorder(pluginMetrics);
    }

    /**
     * Tests basic startSubscription success metric recording.
     */
    @Test
    void testRecordStartSubscriptionSuccess() {
        office365MetricsRecorder.recordStartSubscriptionSuccess();

        verify(startSubscriptionSuccessCounter, times(1)).increment();
    }

    /**
     * Tests multiple startSubscription success metric recordings.
     */
    @Test
    void testRecordStartSubscriptionSuccessMultiple() {
        office365MetricsRecorder.recordStartSubscriptionSuccess();
        office365MetricsRecorder.recordStartSubscriptionSuccess();
        office365MetricsRecorder.recordStartSubscriptionSuccess();

        verify(startSubscriptionSuccessCounter, times(3)).increment();
    }

    /**
     * Tests basic startSubscription failure metric recording.
     */
    @Test
    void testRecordStartSubscriptionFailure() {
        office365MetricsRecorder.recordStartSubscriptionFailure();

        verify(startSubscriptionFailureCounter, times(1)).increment();
    }

    /**
     * Tests multiple startSubscription failure metric recordings.
     */
    @Test
    void testRecordStartSubscriptionFailureMultiple() {
        office365MetricsRecorder.recordStartSubscriptionFailure();
        office365MetricsRecorder.recordStartSubscriptionFailure();

        verify(startSubscriptionFailureCounter, times(2)).increment();
    }

    /**
     * Tests startSubscription latency recording using a Supplier that returns a value.
     */
    @Test
    void testRecordStartSubscriptionLatencyWithSupplier() {
        Supplier<String> operation = () -> "test result";
        when(startSubscriptionLatencyTimer.record(operation)).thenReturn("test result");

        String result = office365MetricsRecorder.recordStartSubscriptionLatency(operation);

        assertEquals("test result", result);
        verify(startSubscriptionLatencyTimer, times(1)).record(operation);
    }

    /**
     * Tests startSubscription latency recording using a Supplier that returns a different type.
     */
    @Test
    void testRecordStartSubscriptionLatencyWithIntegerSupplier() {
        Supplier<Integer> operation = () -> 42;
        when(startSubscriptionLatencyTimer.record(operation)).thenReturn(42);

        Integer result = office365MetricsRecorder.recordStartSubscriptionLatency(operation);

        assertEquals(42, result);
        verify(startSubscriptionLatencyTimer, times(1)).record(operation);
    }

    /**
     * Tests startSubscription latency recording using a Runnable.
     */
    @Test
    void testRecordStartSubscriptionLatencyWithRunnable() {
        Runnable operation = mock(Runnable.class);

        office365MetricsRecorder.recordStartSubscriptionLatency(operation);

        verify(startSubscriptionLatencyTimer, times(1)).record(operation);
    }

    /**
     * Tests startSubscription latency recording using a Duration.
     */
    @Test
    void testRecordStartSubscriptionLatencyWithDuration() {
        Duration duration = Duration.ofMillis(100);

        office365MetricsRecorder.recordStartSubscriptionLatency(duration);

        verify(startSubscriptionLatencyTimer, times(1)).record(duration);
    }

    /**
     * Tests multiple startSubscription latency recordings using Duration.
     */
    @Test
    void testRecordStartSubscriptionLatencyWithMultipleDurations() {
        Duration duration1 = Duration.ofMillis(50);
        Duration duration2 = Duration.ofMillis(150);
        Duration duration3 = Duration.ofMillis(200);

        office365MetricsRecorder.recordStartSubscriptionLatency(duration1);
        office365MetricsRecorder.recordStartSubscriptionLatency(duration2);
        office365MetricsRecorder.recordStartSubscriptionLatency(duration3);

        verify(startSubscriptionLatencyTimer, times(1)).record(duration1);
        verify(startSubscriptionLatencyTimer, times(1)).record(duration2);
        verify(startSubscriptionLatencyTimer, times(1)).record(duration3);
    }

    /**
     * Tests basic startSubscription call metric recording.
     */
    @Test
    void testRecordStartSubscriptionCall() {
        office365MetricsRecorder.recordStartSubscriptionCall();

        verify(startSubscriptionCallsCounter, times(1)).increment();
    }

    /**
     * Tests multiple startSubscription call metric recordings.
     */
    @Test
    void testRecordStartSubscriptionCallMultiple() {
        office365MetricsRecorder.recordStartSubscriptionCall();
        office365MetricsRecorder.recordStartSubscriptionCall();
        office365MetricsRecorder.recordStartSubscriptionCall();
        office365MetricsRecorder.recordStartSubscriptionCall();

        verify(startSubscriptionCallsCounter, times(4)).increment();
    }

    /**
     * Tests constructor initialization of all metrics.
     * Verifies that the correct metric names are used when creating counters and timers.
     */
    @Test
    void testConstructorInitialization() {
        verify(pluginMetrics).counter("startSubscriptionRequestsSuccess");
        verify(pluginMetrics).counter("startSubscriptionRequestsFailed");
        verify(pluginMetrics).timer("startSubscriptionRequestLatency");
        verify(pluginMetrics).counter("startSubscriptionApiCalls");
    }

    /**
     * Tests a comprehensive scenario with mixed metric operations.
     */
    @Test
    void testMixedMetricsScenario() {
        // Record various metrics
        office365MetricsRecorder.recordStartSubscriptionSuccess();
        office365MetricsRecorder.recordStartSubscriptionFailure();
        office365MetricsRecorder.recordStartSubscriptionCall();
        office365MetricsRecorder.recordStartSubscriptionCall();

        // Verify all metrics were recorded correctly
        verify(startSubscriptionSuccessCounter, times(1)).increment();
        verify(startSubscriptionFailureCounter, times(1)).increment();
        verify(startSubscriptionCallsCounter, times(2)).increment();
    }

    /**
     * Tests startSubscription latency recording with a successful supplier operation.
     */
    @Test
    void testRecordStartSubscriptionLatencySuccessfulOperation() {
        Supplier<String> operation = () -> "success";
        String result = "success";
        when(startSubscriptionLatencyTimer.record(operation)).thenReturn(result);

        String returnedResult = office365MetricsRecorder.recordStartSubscriptionLatency(operation);

        assertEquals(result, returnedResult);
        verify(startSubscriptionLatencyTimer, times(1)).record(operation);
    }

    /**
     * Tests startSubscription latency recording with a Duration and verifies timer interaction.
     */
    @Test
    void testRecordStartSubscriptionLatencyDurationTiming() {
        Duration duration = Duration.ofMillis(250);

        office365MetricsRecorder.recordStartSubscriptionLatency(duration);

        verify(startSubscriptionLatencyTimer, times(1)).record(duration);
    }

    /**
     * Tests a realistic scenario simulating multiple startSubscription operations.
     */
    @Test
    void testRealisticStartSubscriptionMetricsScenario() {
        // Simulate 10 startSubscription operations with mixed success/failure
        for (int i = 0; i < 10; i++) {
            office365MetricsRecorder.recordStartSubscriptionCall();
            if (i % 2 == 0) {
                office365MetricsRecorder.recordStartSubscriptionSuccess();
            } else {
                office365MetricsRecorder.recordStartSubscriptionFailure();
            }
        }

        // Verify metrics
        verify(startSubscriptionCallsCounter, times(10)).increment();
        verify(startSubscriptionSuccessCounter, times(5)).increment(); // Even indices: 0,2,4,6,8
        verify(startSubscriptionFailureCounter, times(5)).increment();  // Odd indices: 1,3,5,7,9
    }

    /**
     * Tests that exceptions thrown during latency recording are propagated correctly.
     */
    @Test
    void testRecordStartSubscriptionLatencyPropagatesExceptions() {
        Supplier<String> failingOperation = () -> {
            throw new RuntimeException("Test exception");
        };

        // Configure the mock timer to actually execute the supplier and propagate the exception
        when(startSubscriptionLatencyTimer.record(failingOperation)).thenThrow(new RuntimeException("Test exception"));

        assertThrows(RuntimeException.class, () -> {
            office365MetricsRecorder.recordStartSubscriptionLatency(failingOperation);
        });

        // Timer should still be called even if the operation fails
        verify(startSubscriptionLatencyTimer, times(1)).record(failingOperation);
    }

    /**
     * Tests metric recording with null values where applicable.
     */
    @Test
    void testStartSubscriptionLatencyWithNullDuration() {
        // Duration should not be null in normal usage, but testing robustness
        Duration nullDuration = null;

        // Configure the mock timer to throw NPE when null duration is passed
        doThrow(new NullPointerException()).when(startSubscriptionLatencyTimer).record(nullDuration);

        assertThrows(NullPointerException.class, () -> {
            office365MetricsRecorder.recordStartSubscriptionLatency(nullDuration);
        });
    }
}
