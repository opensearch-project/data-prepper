/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.utils;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.util.Map;
import java.util.Optional;
import java.util.HashMap;
/**
 * The MetricsHelper class.
 */
public class MetricsHelper {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsHelper.class);

    // specific retryable/non-retryable metric names
    private static final String REQUEST_ACCESS_DENIED  = "requestAccessDenied";
    private static final String REQUEST_THROTTLED = "requestThrottled";
    private static final String RESOURCE_NOT_FOUND = "resourceNotFound";

    // specific API metric names
    private static final String GET_REQUESTS_FAILED = "getRequestsFailed";
    private static final String GET_REQUESTS_SUCCESS = "getRequestsSuccess";
    private static final String GET_RESPONSE_SIZE = "getResponseSizeBytes";
    private static final String SEARCH_REQUESTS_FAILED = "searchRequestsFailed";
    private static final String SEARCH_REQUESTS_SUCCESS = "searchRequestsSuccess";
    private static final String SEARCH_RESPONSE_SIZE = "searchResponseSizeBytes";

    // other errors in crawlerClient
    public static final String REQUEST_ERRORS = "requestErrors";

    /**
     * Get the metric counter map for specific errorType
     * FORBIDDEN/UNAUTHORIZED = requestAccessDenied
     * TOO_MANY_REQUESTS = requestThrottled
     * NOT_FOUND = resourceNotFound
     * @param pluginMetrics - metric object class to initialize metric counters
     *
     * @return errorTypeMetricCounterMap
     */
    public static Map<String, Counter> getErrorTypeMetricCounterMap(PluginMetrics pluginMetrics) {
        Map<String, Counter> errorTypeMetricCounterMap = new HashMap<>();
        errorTypeMetricCounterMap.put(HttpStatus.FORBIDDEN.getReasonPhrase(), pluginMetrics.counter(REQUEST_ACCESS_DENIED));
        errorTypeMetricCounterMap.put(HttpStatus.UNAUTHORIZED.getReasonPhrase(), pluginMetrics.counter(REQUEST_ACCESS_DENIED));
        errorTypeMetricCounterMap.put(HttpStatus.TOO_MANY_REQUESTS.getReasonPhrase(), pluginMetrics.counter(REQUEST_THROTTLED));
        errorTypeMetricCounterMap.put(HttpStatus.NOT_FOUND.getReasonPhrase(), pluginMetrics.counter(RESOURCE_NOT_FOUND));
        return errorTypeMetricCounterMap;
    }

    /**
     * Increment the errorType metric if it exists in errorTypeMetricCounterMap
     * Should only be the following:
     * FORBIDDEN/UNAUTHORIZED = requestAccessDenied
     * TOO_MANY_REQUESTS = requestThrottled
     * NOT_FOUND = resourceNotFound
     *
     * @param ex - exception from RestClient
     * @param errorTypeMetricCounterMap - the map of errorType to metric counter
     */
    public static void publishErrorTypeMetricCounter(Exception ex, Map<String, Counter> errorTypeMetricCounterMap) {
        Optional<String> statusCode = Optional.empty();
        if (ex instanceof HttpClientErrorException) {
            HttpClientErrorException httpE = (HttpClientErrorException) ex;
            statusCode = Optional.ofNullable(httpE.getStatusCode().getReasonPhrase());
        } else if (ex instanceof HttpServerErrorException) {
            HttpServerErrorException httpE = (HttpServerErrorException) ex;
            statusCode = Optional.ofNullable(httpE.getStatusCode().getReasonPhrase());
        } else if (ex instanceof SecurityException) { // FORBIDDEN throws SecurityException in RetryHandler
            statusCode = Optional.ofNullable(HttpStatus.FORBIDDEN.getReasonPhrase());
        } // ignore for others

        if (statusCode.isPresent()) {
            String errorType = statusCode.get();
            if (errorTypeMetricCounterMap != null && errorTypeMetricCounterMap.containsKey(errorType)) {
                errorTypeMetricCounterMap.get(errorType).increment();
            }
        }

    }



    /**
     * Records the size of an API response in bytes as a distribution summary metric.
     *
     * This overloaded method handles generic ResponseEntity types (e.g., ResponseEntity&lt;List&lt;Map&lt;String, Object&gt;&gt;&gt;)
     * by using the Content-Length header from the HTTP response. Designed to work with any ResponseEntity type.
     *
     * @param pluginMetrics the PluginMetrics instance used to create and manage the distribution summary
     * @param response the HTTP response entity containing headers from the API call
     *
     * @implNote If the response or response body is null, a value of -1 is recorded to match
     *           the pattern used by HttpEntity.getContentLength() which returns -1 when the
     *           content length is not known. This allows for consistent tracking of responses
     *           where size information is unavailable.
     *
     * @see DistributionSummary#record(double) for how the metric value is recorded
     */
    public static void publishSearchResponseSizeMetricInBytes(PluginMetrics pluginMetrics, ResponseEntity<?> response){
        DistributionSummary summary = pluginMetrics.summary(SEARCH_RESPONSE_SIZE);
        if(response != null && response.getBody() != null){
            summary.record(response.getHeaders().getContentLength());
        } else {
            LOG.error("Response or response body is null when recording API response size metric");
            summary.record(-1L);
        }
    }

    /**
     * Records the size of an API response String in bytes as a distribution summary metric.
     *
     * This overloaded method calculates the size in bytes of the response string content
     * and records it as a metric to help monitor API response sizes for performance analysis
     * and capacity planning.
     *
     * @param pluginMetrics the PluginMetrics instance used to create and manage the distribution summary
     * @param response the response string content from the API call
     *
     * @implNote If the response is null, a value of -1 is recorded for consistency.
     *           The size is calculated using UTF-8 encoding to get accurate byte count.
     *
     * @see DistributionSummary#record(double) for how the metric value is recorded
     */
    public static void publishSearchResponseSizeMetricInBytes(PluginMetrics pluginMetrics, String response){
        DistributionSummary summary = pluginMetrics.summary(SEARCH_RESPONSE_SIZE);
        if(response != null){
            summary.record(response.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        } else {
            LOG.error("Response is null when recording API response size metric");
            summary.record(-1L);
        }
    }

    /**
     * Records a successful search request by incrementing the predefined success counter.
     *
     * This method is used to track successful search API calls for monitoring API health,
     * success rates, and overall system performance metrics. Uses the hardcoded metric
     * name "searchRequestsSuccess" for consistency across the application.
     *
     * @param pluginMetrics the PluginMetrics instance used to create and manage the counter
     *
     * @see Counter#increment() for how the metric value is incremented
     */
    public static void publishSearchRequestsSuccessMetric(PluginMetrics pluginMetrics) {
        Counter successCounter = pluginMetrics.counter(SEARCH_REQUESTS_SUCCESS);
        successCounter.increment();
    }

    /**
     * Provides a failure counter for search requests to be incremented by the caller.
     *
     * This method is used to get the failure counter for search API calls for monitoring API health,
     * failure rates, and identifying system issues. Uses the hardcoded metric name
     * "searchRequestsFailed" for consistency. The caller (such as RetryHandler) is responsible
     * for incrementing the counter when appropriate.
     *
     * @param pluginMetrics the PluginMetrics instance used to create and manage the counter
     * @return the failure counter to be incremented by caller
     */
    public static Counter provideSearchRequestFailureCounter(PluginMetrics pluginMetrics) {
        Counter failureCounter = pluginMetrics.counter(SEARCH_REQUESTS_FAILED);
        return failureCounter;
    }



    /**
     * Records the size of an individual GET request response in bytes as a distribution summary metric.
     *
     * This overloaded method handles generic ResponseEntity types (e.g., ResponseEntity&lt;List&lt;Map&lt;String, Object&gt;&gt;&gt;)
     * by using the Content-Length header from the HTTP response. Designed to work with any ResponseEntity type.
     *
     * @param pluginMetrics the PluginMetrics instance used to create and manage the distribution summary
     * @param response the HTTP response entity containing headers from the GET API call
     *
     * @implNote If the response or response body is null, a value of -1 is recorded to match
     *           the pattern used by HttpEntity.getContentLength() which returns -1 when the
     *           content length is not known. This allows for consistent tracking of responses
     *           where size information is unavailable.
     *
     * @see DistributionSummary#record(double) for how the metric value is recorded
     */
    public static void publishGetResponseSizeMetricInBytes(PluginMetrics pluginMetrics, ResponseEntity<?> response) {
        DistributionSummary summary = pluginMetrics.summary(GET_RESPONSE_SIZE);
        if(response != null && response.getBody() != null){
            summary.record(response.getHeaders().getContentLength());
        } else {
            LOG.error("Response or response body is null when recording GET request response size metric");
            summary.record(-1L);
        }
    }

    /**
     * Records the size of an individual GET request response String in bytes as a distribution summary metric.
     *
     * This overloaded method calculates the size in bytes of the response string content
     * and records it as a metric to help monitor individual GET request response sizes for performance analysis
     * and capacity planning.
     *
     * @param pluginMetrics the PluginMetrics instance used to create and manage the distribution summary
     * @param response the response string content from the GET API call
     *
     * @implNote If the response is null, a value of -1 is recorded for consistency.
     *           The size is calculated using UTF-8 encoding to get accurate byte count.
     *
     * @see DistributionSummary#record(double) for how the metric value is recorded
     */
    public static void publishGetResponseSizeMetricInBytes(PluginMetrics pluginMetrics, String response){
        DistributionSummary summary = pluginMetrics.summary(GET_RESPONSE_SIZE);
        if(response != null){
            summary.record(response.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        } else {
            LOG.error("Response is null when recording GET request response size metric");
            summary.record(-1L);
        }
    }

    /**
     * Records a successful individual GET request by incrementing the predefined success counter.
     *
     * This method is used to track successful individual GET API calls for monitoring API health,
     * success rates, and overall system performance metrics. Uses the hardcoded metric
     * name "getRequestsSuccess" for consistency across the application.
     *
     * @param pluginMetrics the PluginMetrics instance used to create and manage the counter
     *
     * @see Counter#increment() for how the metric value is incremented
     */
    public static void publishGetRequestsSuccessMetric(PluginMetrics pluginMetrics) {
        Counter successCounter = pluginMetrics.counter(GET_REQUESTS_SUCCESS);
        successCounter.increment();
    }

    /**
     * Provides a failure counter for GET requests to be incremented by the caller.
     *
     * This method is used to get the failure counter for individual GET API calls for monitoring API health,
     * failure rates, and identifying system issues. Uses the hardcoded metric name
     * "getRequestsFailed" for consistency. The caller (such as RetryHandler) is responsible
     * for incrementing the counter when appropriate.
     *
     * @param pluginMetrics the PluginMetrics instance used to create and manage the counter
     * @return the failure counter to be incremented by caller
     */
    public static Counter provideGetRequestsFailureCounter(PluginMetrics pluginMetrics) {
        Counter failureCounter = pluginMetrics.counter(GET_REQUESTS_FAILED);
        return failureCounter;
    }
}
