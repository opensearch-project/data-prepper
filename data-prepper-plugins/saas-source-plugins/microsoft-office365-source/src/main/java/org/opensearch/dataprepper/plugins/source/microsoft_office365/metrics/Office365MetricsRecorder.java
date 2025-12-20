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
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Microsoft Office 365 specific metrics recorder for Office 365 APIs that are unique to Office 365.
 * 
 * This class records metrics for Office 365 specific API operations that are not found in other vendor APIs:
 * - StartSubscription latency: Time taken to start/manage Office 365 audit log subscriptions
 * - StartSubscription success/failure rates: Track successful and failed subscription operations
 * - StartSubscription call counts: Track individual API calls made for subscription management
 * 
 * This is separate from the shared VendorAPIMetricsRecorder to isolate Office 365-specific
 * API metrics that are unique to the Office 365 Management API within the M365 module.
 * 
 * NOTE: Any new Office 365 specific metrics should be implemented here to maintain proper
 * separation of vendor-specific functionality and keep Office 365 metrics centralized.
 */
public class Office365MetricsRecorder {

    // StartSubscription operation metrics
    private final Counter startSubscriptionSuccessCounter;
    private final Counter startSubscriptionFailureCounter;
    private final Timer startSubscriptionLatencyTimer;
    private final Counter startSubscriptionCallsCounter;

    /**
     * Creates an Office365MetricsRecorder for startSubscription-specific metrics.
     * 
     * @param pluginMetrics The plugin metrics instance
     */
    public Office365MetricsRecorder(PluginMetrics pluginMetrics) {
        // Initialize startSubscription metrics without office365 prefix
        this.startSubscriptionSuccessCounter = pluginMetrics.counter("startSubscriptionRequestsSuccess");
        this.startSubscriptionFailureCounter = pluginMetrics.counter("startSubscriptionRequestsFailed");
        this.startSubscriptionLatencyTimer = pluginMetrics.timer("startSubscriptionRequestLatency");
        this.startSubscriptionCallsCounter = pluginMetrics.counter("startSubscriptionApiCalls");
    }

    /**
     * Records a successful startSubscription operation.
     */
    public void recordStartSubscriptionSuccess() {
        startSubscriptionSuccessCounter.increment();
    }

    /**
     * Records a failed startSubscription operation.
     */
    public void recordStartSubscriptionFailure() {
        startSubscriptionFailureCounter.increment();
    }

    /**
     * Records the latency of a startSubscription operation using a Supplier.
     * 
     * @param operation The operation to time
     * @param <T> The return type of the operation
     * @return The result of the operation
     */
    public <T> T recordStartSubscriptionLatency(Supplier<T> operation) {
        return startSubscriptionLatencyTimer.record(operation);
    }

    /**
     * Records the latency of a startSubscription operation using a Runnable.
     * 
     * @param operation The operation to time
     */
    public void recordStartSubscriptionLatency(Runnable operation) {
        startSubscriptionLatencyTimer.record(operation);
    }

    /**
     * Records the latency of a startSubscription operation using a Duration.
     * 
     * @param duration The duration to record
     */
    public void recordStartSubscriptionLatency(Duration duration) {
        startSubscriptionLatencyTimer.record(duration);
    }

    /**
     * Records an individual startSubscription API call.
     * This tracks the number of individual API requests made during startSubscription operations.
     */
    public void recordStartSubscriptionCall() {
        startSubscriptionCallsCounter.increment();
    }
}
