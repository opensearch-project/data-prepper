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
 * - General API operations: request counts, logs requested, error categorization
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

    // Shared metrics
    private final Counter totalDataApiRequestsCounter;
    private final Counter logsRequestedCounter;
    
    // Error metrics
    private final Counter requestAccessDeniedCounter;
    private final Counter requestThrottledCounter;
    private final Counter resourceNotFoundCounter;
    
    private final PluginMetrics pluginMetrics;

    /**
     * Creates a unified VendorAPIMetricsRecorder with all operation types.
     * 
     * @param pluginMetrics The plugin metrics instance
     */
    public VendorAPIMetricsRecorder(PluginMetrics pluginMetrics) {
        this.pluginMetrics = pluginMetrics;
        
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
