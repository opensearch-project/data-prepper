/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Comprehensive metrics recorder for vendor API operations in SaaS source plugins.
 * 
 * This class provides a unified interface for recording metrics across different types of vendor API operations:
 * - Search operations: latency, success/failure rates, and response sizes
 * - Get/retrieval operations: latency, success/failure rates, and response sizes  
 * - Authentication operations: latency, success/failure rates
 * - Subscription operations: latency, success/failure rates, and call counts
 * - General API operations: request counts, logs requested, error categorization
 * 
 * <h3>Subscription Metrics Gating</h3>
 * The subscription metrics collection can be controlled via the constructor parameter. This gating mechanism 
 * is introduced to allow subscription metrics for vendors that support them while reducing overhead for 
 * vendors that don't require these vendor-dependent metrics. When disabled, subscription metrics are not 
 * created and method calls become no-ops, providing significant performance benefits for vendors that don't 
 * utilize subscription-based operations.
 * 
 * Most methods return void for efficient standalone usage. The error() method supports chaining for error handling scenarios.
 */
public class VendorAPIMetricsRecorder {

    // Search operation metrics
    private final Counter searchSuccessCounter;
    private final Counter searchFailureCounter;
    private final Timer searchLatencyTimer;
    private final DistributionSummary searchResponseSizeSummary;

    // Get operation metrics  
    private final Counter getSuccessCounter;
    private final Counter getFailureCounter;
    private final Timer getLatencyTimer;
    private final DistributionSummary getResponseSizeSummary;

    // Authentication operation metrics
    private final Counter authSuccessCounter;
    private final Counter authFailureCounter;
    private final Timer authLatencyTimer;

    // Start subscription operation metrics
    private final Counter subscriptionSuccessCounter;
    private final Counter subscriptionFailureCounter;
    private final Timer subscriptionLatencyTimer;
    private final Counter subscriptionCallsCounter;

    // List subscription operation metrics
    private final Counter listSubscriptionSuccessCounter;
    private final Counter listSubscriptionFailureCounter;
    private final Timer listSubscriptionLatencyTimer;
    private final Counter listSubscriptionCallsCounter;

    // Shared metrics
    private final Counter totalDataApiRequestsCounter;
    private final Counter logsRequestedCounter;
    
    // Error metrics
    private final Counter requestAccessDeniedCounter;
    private final Counter requestThrottledCounter;
    private final Counter resourceNotFoundCounter;
    
    private final PluginMetrics pluginMetrics;
    private final boolean enableSubscriptionMetrics;

    /**
     * Creates a unified VendorAPIMetricsRecorder with all operation types.
     * Subscription metrics are enabled by default for backward compatibility.
     * 
     * @param pluginMetrics The plugin metrics instance
     */
    public VendorAPIMetricsRecorder(PluginMetrics pluginMetrics) {
        this(pluginMetrics, false);
    }

    /**
     * Creates a unified VendorAPIMetricsRecorder with configurable subscription metrics.
     * 
     * @param pluginMetrics The plugin metrics instance
     * @param enableSubscriptionMetrics Whether to enable subscription metrics collection
     */
    public VendorAPIMetricsRecorder(PluginMetrics pluginMetrics, boolean enableSubscriptionMetrics) {
        this.pluginMetrics = pluginMetrics;
        this.enableSubscriptionMetrics = enableSubscriptionMetrics;
        
        // Search metrics
        this.searchSuccessCounter = pluginMetrics.counter("searchRequestsSuccess");
        this.searchFailureCounter = pluginMetrics.counter("searchRequestsFailed");
        this.searchLatencyTimer = pluginMetrics.timer("searchRequestLatency");
        this.searchResponseSizeSummary = pluginMetrics.summary("searchResponseSizeBytes");

        // Get metrics
        this.getSuccessCounter = pluginMetrics.counter("getRequestsSuccess");
        this.getFailureCounter = pluginMetrics.counter("getRequestsFailed");
        this.getLatencyTimer = pluginMetrics.timer("getRequestLatency");
        this.getResponseSizeSummary = pluginMetrics.summary("getResponseSizeBytes");

        // Auth metrics  
        this.authSuccessCounter = pluginMetrics.counter("authenticationRequestsSuccess");
        this.authFailureCounter = pluginMetrics.counter("authenticationRequestsFailed");
        this.authLatencyTimer = pluginMetrics.timer("authenticationRequestLatency");

        // Conditionally initialize subscription metrics based on enableSubscriptionMetrics flag
        if (enableSubscriptionMetrics) {
            // Start subscription metrics
            this.subscriptionSuccessCounter = pluginMetrics.counter("startSubscriptionRequestsSuccess");
            this.subscriptionFailureCounter = pluginMetrics.counter("startSubscriptionRequestsFailed");
            this.subscriptionLatencyTimer = pluginMetrics.timer("startSubscriptionRequestLatency");
            this.subscriptionCallsCounter = pluginMetrics.counter("startSubscriptionApiCalls");

            // List subscription metrics
            this.listSubscriptionSuccessCounter = pluginMetrics.counter("listSubscriptionRequestsSuccess");
            this.listSubscriptionFailureCounter = pluginMetrics.counter("listSubscriptionRequestsFailed");
            this.listSubscriptionLatencyTimer = pluginMetrics.timer("listSubscriptionRequestLatency");
            this.listSubscriptionCallsCounter = pluginMetrics.counter("listSubscriptionApiCalls");
        } else {
            // Use no-op implementations when subscription metrics are disabled
            this.subscriptionSuccessCounter = null;
            this.subscriptionFailureCounter = null;
            this.subscriptionLatencyTimer = null;
            this.subscriptionCallsCounter = null;

            this.listSubscriptionSuccessCounter = null;
            this.listSubscriptionFailureCounter = null;
            this.listSubscriptionLatencyTimer = null;
            this.listSubscriptionCallsCounter = null;
        }

        // Shared metrics
        this.totalDataApiRequestsCounter = pluginMetrics.counter("totalDataApiRequests");
        this.logsRequestedCounter = pluginMetrics.counter("logsRequested");
        
        // Error metrics
        this.requestAccessDeniedCounter = pluginMetrics.counter("requestAccessDenied");
        this.requestThrottledCounter = pluginMetrics.counter("requestThrottled");
        this.resourceNotFoundCounter = pluginMetrics.counter("resourceNotFound");
    }

    // Search operation methods
    public void recordSearchSuccess() {
        searchSuccessCounter.increment();
    }

    public void recordSearchFailure() {
        searchFailureCounter.increment();
    }

    public <T> T recordSearchLatency(Supplier<T> operation) {
        return searchLatencyTimer.record(operation);
    }

    public void recordSearchLatency(Runnable operation) {
        searchLatencyTimer.record(operation);
    }

    public void recordSearchLatency(Duration duration) {
        searchLatencyTimer.record(duration);
    }

    public void recordSearchResponseSize(ResponseEntity<?> response) {
        if (response != null) {
            String contentLength = response.getHeaders().getFirst("Content-Length");
            if (contentLength != null) {
                try {
                    searchResponseSizeSummary.record(Long.parseLong(contentLength));
                } catch (NumberFormatException e) {
                    searchResponseSizeSummary.record(0L);
                }
            } else {
                searchResponseSizeSummary.record(0L);
            }
        } else {
            searchResponseSizeSummary.record(0L);
        }
    }

    public void recordSearchResponseSize(long bytes) {
        searchResponseSizeSummary.record(bytes);
    }

    public void recordSearchResponseSize(String response) {
        if (response != null) {
            searchResponseSizeSummary.record(response.getBytes().length);
        } else {
            searchResponseSizeSummary.record(0L);
        }
    }

    // Get operation methods
    public void recordGetSuccess() {
        getSuccessCounter.increment();
    }

    public void recordGetFailure() {
        getFailureCounter.increment();
    }

    public <T> T recordGetLatency(Supplier<T> operation) {
        return getLatencyTimer.record(operation);
    }

    public void recordGetLatency(Runnable operation) {
        getLatencyTimer.record(operation);
    }

    public void recordGetLatency(Duration duration) {
        getLatencyTimer.record(duration);
    }

    public void recordGetResponseSize(String response) {
        if (response != null) {
            getResponseSizeSummary.record(response.getBytes().length);
        } else {
            getResponseSizeSummary.record(0L);
        }
    }

    public void recordGetResponseSize(long bytes) {
        getResponseSizeSummary.record(bytes);
    }

    public void recordGetResponseSize(ResponseEntity<?> response) {
        if (response != null && response.getBody() != null) {
            getResponseSizeSummary.record(response.getHeaders().getContentLength());
        } else {
            getResponseSizeSummary.record(0L);
        }
    }

    // Authentication operation methods
    public void recordAuthSuccess() {
        authSuccessCounter.increment();
    }

    public void recordAuthFailure() {
        authFailureCounter.increment();
    }

    public <T> T recordAuthLatency(Supplier<T> operation) {
        return authLatencyTimer.record(operation);
    }

    public void recordAuthLatency(Runnable operation) {
        authLatencyTimer.record(operation);
    }

    public void recordAuthLatency(Duration duration) {
        authLatencyTimer.record(duration);
    }

    // Subscription operation methods
    public void recordSubscriptionSuccess() {
        if (enableSubscriptionMetrics && subscriptionSuccessCounter != null) {
            subscriptionSuccessCounter.increment();
        }
    }

    public void recordSubscriptionFailure() {
        if (enableSubscriptionMetrics && subscriptionFailureCounter != null) {
            subscriptionFailureCounter.increment();
        }
    }

    public <T> T recordSubscriptionLatency(Supplier<T> operation) {
        if (enableSubscriptionMetrics && subscriptionLatencyTimer != null) {
            return subscriptionLatencyTimer.record(operation);
        } else {
            // Execute operation without recording metrics
            return operation.get();
        }
    }

    public void recordSubscriptionLatency(Runnable operation) {
        if (enableSubscriptionMetrics && subscriptionLatencyTimer != null) {
            subscriptionLatencyTimer.record(operation);
        } else {
            // Execute operation without recording metrics
            operation.run();
        }
    }

    public void recordSubscriptionLatency(Duration duration) {
        if (enableSubscriptionMetrics && subscriptionLatencyTimer != null) {
            subscriptionLatencyTimer.record(duration);
        }
    }

    public void recordSubscriptionCall() {
        if (enableSubscriptionMetrics && subscriptionCallsCounter != null) {
            subscriptionCallsCounter.increment();
        }
    }

    // List subscription operation methods
    public void recordListSubscriptionSuccess() {
        if (enableSubscriptionMetrics && listSubscriptionSuccessCounter != null) {
            listSubscriptionSuccessCounter.increment();
        }
    }

    public void recordListSubscriptionFailure() {
        if (enableSubscriptionMetrics && listSubscriptionFailureCounter != null) {
            listSubscriptionFailureCounter.increment();
        }
    }

    public <T> T recordListSubscriptionLatency(Supplier<T> operation) {
        if (enableSubscriptionMetrics && listSubscriptionLatencyTimer != null) {
            return listSubscriptionLatencyTimer.record(operation);
        } else {
            // Execute operation without recording metrics
            return operation.get();
        }
    }

    public void recordListSubscriptionLatency(Runnable operation) {
        if (enableSubscriptionMetrics && listSubscriptionLatencyTimer != null) {
            listSubscriptionLatencyTimer.record(operation);
        } else {
            // Execute operation without recording metrics
            operation.run();
        }
    }

    public void recordListSubscriptionLatency(Duration duration) {
        if (enableSubscriptionMetrics && listSubscriptionLatencyTimer != null) {
            listSubscriptionLatencyTimer.record(duration);
        }
    }

    public void recordListSubscriptionCall() {
        if (enableSubscriptionMetrics && listSubscriptionCallsCounter != null) {
            listSubscriptionCallsCounter.increment();
        }
    }

    // Shared operation methods
    public void recordDataApiRequest() {
        totalDataApiRequestsCounter.increment();
    }

    public void recordLogsRequested() {
        logsRequestedCounter.increment();
    }

    /**
     * Records error metrics based on exception type and HTTP status code.
     * Maps specific HTTP errors to business-meaningful metrics:
     * - 401/403 -> requestAccessDenied
     * - 429 -> requestThrottled  
     * - 404 -> resourceNotFound
     * - SecurityException -> requestAccessDenied (treated as FORBIDDEN)
     * 
     * @param exception The exception that occurred
     */
    public void recordError(Exception exception) {
        if (exception instanceof HttpClientErrorException) {
            HttpClientErrorException httpE = (HttpClientErrorException) exception;
            HttpStatus status = httpE.getStatusCode();
            
            if (HttpStatus.FORBIDDEN == status || HttpStatus.UNAUTHORIZED == status) {
                requestAccessDeniedCounter.increment();
            } else if (HttpStatus.TOO_MANY_REQUESTS == status) {
                requestThrottledCounter.increment();
            } else if (HttpStatus.NOT_FOUND == status) {
                resourceNotFoundCounter.increment();
            }
        } else if (exception instanceof SecurityException) {
            requestAccessDeniedCounter.increment();
        }
    }
}
