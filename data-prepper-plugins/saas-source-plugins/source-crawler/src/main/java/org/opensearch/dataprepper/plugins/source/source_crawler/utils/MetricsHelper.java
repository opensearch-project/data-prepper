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
import org.springframework.http.HttpStatus;
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

    // specific retryable/non-retryable metric names
    private static final String REQUEST_ACCESS_DENIED  = "requestAccessDenied";
    private static final String REQUEST_THROTTLED = "requestThrottled";
    private static final String RESOURCE_NOT_FOUND = "resourceNotFound";

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
}