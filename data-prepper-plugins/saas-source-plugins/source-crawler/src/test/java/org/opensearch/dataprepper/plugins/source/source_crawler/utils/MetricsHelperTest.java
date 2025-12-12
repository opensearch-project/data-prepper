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
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.Mockito.lenient;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MetricsHelperTest {

    // Private test constants - duplicated from MetricsHelper for test isolation
    private static final String REQUEST_ACCESS_DENIED = "requestAccessDenied";
    private static final String REQUEST_THROTTLED = "requestThrottled";
    private static final String RESOURCE_NOT_FOUND = "resourceNotFound";
    private static final String GET_REQUESTS_FAILED = "getRequestsFailed";
    private static final String GET_REQUESTS_SUCCESS = "getRequestsSuccess";
    private static final String GET_RESPONSE_SIZE = "getResponseSizeBytes";
    private static final String SEARCH_REQUESTS_FAILED = "searchRequestsFailed";
    private static final String SEARCH_REQUESTS_SUCCESS = "searchRequestsSuccess";
    private static final String SEARCH_RESPONSE_SIZE = "searchResponseSizeBytes";
    private static final String LOGS_REQUESTED = "logsRequested";
    private static final String GET_REQUEST_LATENCY = "getRequestLatency";
    private static final String SEARCH_CALL_LATENCY = "searchCallLatency";
    private static final String TOTAL_API_REQUESTS = "totalApiRequests";

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter mockCounter;

    @Mock
    private DistributionSummary mockDistributionSummary;

    @Mock
    private Timer mockTimer;

    @BeforeEach
    void setUp() {
        lenient().when(pluginMetrics.counter(anyString())).thenReturn(mockCounter);
        lenient().when(pluginMetrics.summary(anyString())).thenReturn(mockDistributionSummary);
        lenient().when(pluginMetrics.timer(anyString())).thenReturn(mockTimer);
    }

    @Test
    void testGetErrorTypeMetricCounterMap() {
        Map<String, Counter> result = MetricsHelper.getErrorTypeMetricCounterMap(pluginMetrics);

        assertNotNull(result);
        assertEquals(4, result.size());

        // Verify expected HTTP status mappings
        assertTrue(result.containsKey(HttpStatus.FORBIDDEN.getReasonPhrase()));
        assertTrue(result.containsKey(HttpStatus.UNAUTHORIZED.getReasonPhrase()));
        assertTrue(result.containsKey(HttpStatus.TOO_MANY_REQUESTS.getReasonPhrase()));
        assertTrue(result.containsKey(HttpStatus.NOT_FOUND.getReasonPhrase()));

        result.values().forEach(counter -> assertEquals(mockCounter, counter));

        // requestAccessDenied is called twice for FORBIDDEN and UNAUTHORIZED
        verify(pluginMetrics, times(2)).counter(REQUEST_ACCESS_DENIED);
        verify(pluginMetrics).counter(REQUEST_THROTTLED);
        verify(pluginMetrics).counter(RESOURCE_NOT_FOUND);
    }

    @Test
    void testPublishErrorTypeMetricCounterWithHttpClientErrorException() {
        Map<String, Counter> errorTypeMetricCounterMap = MetricsHelper.getErrorTypeMetricCounterMap(pluginMetrics);
        HttpClientErrorException exception = new HttpClientErrorException(HttpStatus.FORBIDDEN);

        MetricsHelper.publishErrorTypeMetricCounter(exception, errorTypeMetricCounterMap);

        verify(mockCounter).increment();
    }

    @Test
    void testPublishErrorTypeMetricCounterWithHttpServerErrorException() {
        Map<String, Counter> errorTypeMetricCounterMap = MetricsHelper.getErrorTypeMetricCounterMap(pluginMetrics);
        HttpServerErrorException exception = new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR);

        MetricsHelper.publishErrorTypeMetricCounter(exception, errorTypeMetricCounterMap);

        // Should not increment because INTERNAL_SERVER_ERROR is not in the supported error map
        verify(mockCounter, never()).increment();
    }

    @Test
    void testPublishErrorTypeMetricCounterWithSecurityException() {
        Map<String, Counter> errorTypeMetricCounterMap = MetricsHelper.getErrorTypeMetricCounterMap(pluginMetrics);
        SecurityException exception = new SecurityException("Access denied");

        MetricsHelper.publishErrorTypeMetricCounter(exception, errorTypeMetricCounterMap);

        verify(mockCounter).increment();
    }

    @Test
    void testPublishErrorTypeMetricCounterWithUnauthorizedException() {
        Map<String, Counter> errorTypeMetricCounterMap = MetricsHelper.getErrorTypeMetricCounterMap(pluginMetrics);
        HttpClientErrorException exception = new HttpClientErrorException(HttpStatus.UNAUTHORIZED);

        MetricsHelper.publishErrorTypeMetricCounter(exception, errorTypeMetricCounterMap);

        verify(mockCounter).increment();
    }

    @Test
    void testPublishErrorTypeMetricCounterWithTooManyRequestsException() {
        Map<String, Counter> errorTypeMetricCounterMap = MetricsHelper.getErrorTypeMetricCounterMap(pluginMetrics);
        HttpClientErrorException exception = new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS);

        MetricsHelper.publishErrorTypeMetricCounter(exception, errorTypeMetricCounterMap);

        verify(mockCounter).increment();
    }

    @Test
    void testPublishErrorTypeMetricCounterWithNotFoundException() {
        Map<String, Counter> errorTypeMetricCounterMap = MetricsHelper.getErrorTypeMetricCounterMap(pluginMetrics);
        HttpClientErrorException exception = new HttpClientErrorException(HttpStatus.NOT_FOUND);

        MetricsHelper.publishErrorTypeMetricCounter(exception, errorTypeMetricCounterMap);

        verify(mockCounter).increment();
    }

    @Test
    void testPublishErrorTypeMetricCounterWithUnsupportedException() {
        Map<String, Counter> errorTypeMetricCounterMap = MetricsHelper.getErrorTypeMetricCounterMap(pluginMetrics);
        RuntimeException exception = new RuntimeException("Unsupported exception");

        MetricsHelper.publishErrorTypeMetricCounter(exception, errorTypeMetricCounterMap);

        // Should not increment because RuntimeException is not handled
        verify(mockCounter, never()).increment();
    }

    @Test
    void testPublishErrorTypeMetricCounterWithNullMap() {
        HttpClientErrorException exception = new HttpClientErrorException(HttpStatus.FORBIDDEN);

        // Should not throw exception when map is null
        assertDoesNotThrow(() ->
                MetricsHelper.publishErrorTypeMetricCounter(exception, null)
        );
    }

    @Test
    void testPublishErrorTypeMetricCounterWithBadRequestException() {
        Map<String, Counter> errorTypeMetricCounterMap = MetricsHelper.getErrorTypeMetricCounterMap(pluginMetrics);
        HttpClientErrorException exception = new HttpClientErrorException(HttpStatus.BAD_REQUEST);

        MetricsHelper.publishErrorTypeMetricCounter(exception, errorTypeMetricCounterMap);

        // Should not increment because BAD_REQUEST is not in the supported error map
        verify(mockCounter, never()).increment();
    }

    // Data providers for parameterized tests
    static Stream<Arguments> responseEntityTestCases() {
        return Stream.of(
                Arguments.of("ValidResponse", 1024L, "test body", 1024L),
                Arguments.of("NullResponse", null, null, -1L),
                Arguments.of("NullBody", 1024L, null, -1L),
                Arguments.of("ZeroContentLength", 0L, "", 0L),
                Arguments.of("NoContentLength", -1L, "test body", -1L),
                Arguments.of("LargeContentLength", 10_485_760L, "large body", 10_485_760L)
        );
    }

    static Stream<Arguments> stringTestCases() {
        return Stream.of(
                Arguments.of("ValidString", "test response content"),
                Arguments.of("NullString", null),
                Arguments.of("EmptyString", ""),
                Arguments.of("Utf8String", "test with unicode: 测试 and émojis 🎉")
        );
    }

    static Stream<Arguments> metricMethods() {
        return Stream.of(
                Arguments.of("search", SEARCH_RESPONSE_SIZE,
                        (BiConsumer<PluginMetrics, ResponseEntity<?>>) MetricsHelper::publishSearchResponseSizeMetricInBytes,
                        (BiConsumer<PluginMetrics, String>) MetricsHelper::publishSearchResponseSizeMetricInBytes),
                Arguments.of("get", GET_RESPONSE_SIZE,
                        (BiConsumer<PluginMetrics, ResponseEntity<?>>) MetricsHelper::publishGetResponseSizeMetricInBytes,
                        (BiConsumer<PluginMetrics, String>) MetricsHelper::publishGetResponseSizeMetricInBytes)
        );
    }

    @ParameterizedTest(name = "{0} ResponseEntity test: {1}")
    @MethodSource("responseEntityTestCases")
    void testResponseSizeMetricWithResponseEntity(String testName, Long contentLength, String body, long expectedRecord) {
        metricMethods().forEach(args -> {
            String methodType = (String) args.get()[0];
            String metricName = (String) args.get()[1];
            BiConsumer<PluginMetrics, ResponseEntity<?>> method =
                    (BiConsumer<PluginMetrics, ResponseEntity<?>>) args.get()[2];

            reset(pluginMetrics, mockDistributionSummary);
            when(pluginMetrics.summary(anyString())).thenReturn(mockDistributionSummary);

            ResponseEntity<String> response = null;
            if (contentLength != null) {
                HttpHeaders headers = new HttpHeaders();
                if (contentLength >= 0) {
                    headers.setContentLength(contentLength);
                }
                response = new ResponseEntity<>(body, headers, HttpStatus.OK);
            }

            method.accept(pluginMetrics, response);

            verify(pluginMetrics).summary(metricName);
            verify(mockDistributionSummary).record(expectedRecord);
        });
    }

    @ParameterizedTest(name = "{0} String test: {1}")
    @MethodSource("stringTestCases")
    void testResponseSizeMetricWithString(String testName, String responseBody) {
        metricMethods().forEach(args -> {
            String methodType = (String) args.get()[0];
            String metricName = (String) args.get()[1];
            BiConsumer<PluginMetrics, String> method =
                    (BiConsumer<PluginMetrics, String>) args.get()[3];

            reset(pluginMetrics, mockDistributionSummary);
            when(pluginMetrics.summary(anyString())).thenReturn(mockDistributionSummary);

            method.accept(pluginMetrics, responseBody);

            long expectedSize = responseBody != null ?
                    responseBody.getBytes(java.nio.charset.StandardCharsets.UTF_8).length : -1L;

            verify(pluginMetrics).summary(metricName);
            verify(mockDistributionSummary).record(expectedSize);
        });
    }


    @ParameterizedTest(name = "Success metric for {0} requests")
    @MethodSource({"org.opensearch.dataprepper.plugins.source.source_crawler.utils.MetricsHelperTest#successMetricMethods"})
    void testSuccessMetrics(String methodType, String expectedMetricName) {
        if ("search".equals(methodType)) {
            MetricsHelper.publishSearchRequestsSuccessMetric(pluginMetrics);
        } else {
            MetricsHelper.publishGetRequestsSuccessMetric(pluginMetrics);
        }

        verify(pluginMetrics).counter(expectedMetricName);
        verify(mockCounter).increment();
    }

    static Stream<Arguments> successMetricMethods() {
        return Stream.of(
                Arguments.of("search", SEARCH_REQUESTS_SUCCESS),
                Arguments.of("get", GET_REQUESTS_SUCCESS)
        );
    }

    @ParameterizedTest(name = "Failure counter provider for {0} requests")
    @MethodSource({"org.opensearch.dataprepper.plugins.source.source_crawler.utils.MetricsHelperTest#failureCounterMethods"})
    void testFailureCounterProviders(String methodType, String expectedMetricName) {
        Counter result;
        if ("search".equals(methodType)) {
            result = MetricsHelper.provideSearchRequestFailureCounter(pluginMetrics);
        } else {
            result = MetricsHelper.provideGetRequestsFailureCounter(pluginMetrics);
        }

        verify(pluginMetrics).counter(expectedMetricName);
        verify(mockCounter, never()).increment(); // Should NOT increment - RetryHandler's responsibility
        assertEquals(mockCounter, result);
    }

    static Stream<Arguments> failureCounterMethods() {
        return Stream.of(
                Arguments.of("search", SEARCH_REQUESTS_FAILED),
                Arguments.of("get", GET_REQUESTS_FAILED)
        );
    }

    @Test
    void testResponseEntityCompatibilityWithVariousResponseTypes() {
        // Test that ResponseEntity<?> methods work with different ResponseEntity types

        reset(pluginMetrics, mockDistributionSummary);
        when(pluginMetrics.summary(anyString())).thenReturn(mockDistributionSummary);

        String testContent = "test content for ResponseEntity compatibility";

        // Test search methods with ResponseEntity<String>
        HttpHeaders headers = new HttpHeaders();
        headers.setContentLength(1000L);
        ResponseEntity<String> responseEntity = new ResponseEntity<>(testContent, headers, HttpStatus.OK);
        MetricsHelper.publishSearchResponseSizeMetricInBytes(pluginMetrics, responseEntity);

        // Test get methods with ResponseEntity<String>
        headers.setContentLength(2000L);
        ResponseEntity<String> getResponseEntity = new ResponseEntity<>(testContent, headers, HttpStatus.OK);
        MetricsHelper.publishGetResponseSizeMetricInBytes(pluginMetrics, getResponseEntity);

        // Verify both method types use their respective metric names
        verify(pluginMetrics).summary(SEARCH_RESPONSE_SIZE);
        verify(pluginMetrics).summary(GET_RESPONSE_SIZE);

        // Test with different ResponseEntity generic types
        HttpHeaders genericHeaders = new HttpHeaders();
        genericHeaders.setContentLength(500L);
        ResponseEntity<String> stringEntityResponse = new ResponseEntity<>("test string", genericHeaders, HttpStatus.OK);

        assertDoesNotThrow(() -> {
            MetricsHelper.publishSearchResponseSizeMetricInBytes(pluginMetrics, stringEntityResponse);
            MetricsHelper.publishGetResponseSizeMetricInBytes(pluginMetrics, stringEntityResponse);
        });
    }

    @Test
    void testSearchResponseSizeMetricWithGenericResponseEntity() {
        // Test the new ResponseEntity<?> overload that handles ResponseEntity<List<Map<String, Object>>>

        reset(pluginMetrics, mockDistributionSummary);
        when(pluginMetrics.summary(anyString())).thenReturn(mockDistributionSummary);

        // Test with ResponseEntity<List<Map<String, Object>>> like used in OktaSSORestClient
        java.util.List<java.util.Map<String, Object>> eventsList = java.util.Arrays.asList(
                java.util.Map.of("id", "123", "message", "test event"),
                java.util.Map.of("id", "456", "message", "another event")
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentLength(3000L);
        ResponseEntity<java.util.List<java.util.Map<String, Object>>> genericResponse =
                new ResponseEntity<>(eventsList, headers, HttpStatus.OK);

        // This should work with the new ResponseEntity<?> overload
        MetricsHelper.publishSearchResponseSizeMetricInBytes(pluginMetrics, genericResponse);

        verify(pluginMetrics).summary(SEARCH_RESPONSE_SIZE);
        verify(mockDistributionSummary).record(3000L);
    }

    @Test
    void testGetResponseSizeMetricWithGenericResponseEntity() {
        // Test the new ResponseEntity<?> overload for GET requests

        reset(pluginMetrics, mockDistributionSummary);
        when(pluginMetrics.summary(anyString())).thenReturn(mockDistributionSummary);

        // Test with ResponseEntity<List<Map<String, Object>>> like used in OktaSSORestClient
        java.util.List<java.util.Map<String, Object>> eventsList = java.util.Arrays.asList(
                java.util.Map.of("id", "789", "content", "single event detail")
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentLength(1500L);
        ResponseEntity<java.util.List<java.util.Map<String, Object>>> genericResponse =
                new ResponseEntity<>(eventsList, headers, HttpStatus.OK);

        // This should work with the new ResponseEntity<?> overload
        MetricsHelper.publishGetResponseSizeMetricInBytes(pluginMetrics, genericResponse);

        verify(pluginMetrics).summary(GET_RESPONSE_SIZE);
        verify(mockDistributionSummary).record(1500L);
    }

    @Test
    void testGenericResponseEntityOverloadsWithNullValues() {
        // Test the new generic ResponseEntity<?> overloads with null scenarios

        reset(pluginMetrics, mockDistributionSummary);
        when(pluginMetrics.summary(anyString())).thenReturn(mockDistributionSummary);

        // Test null response
        MetricsHelper.publishSearchResponseSizeMetricInBytes(pluginMetrics, (ResponseEntity<?>) null);
        verify(pluginMetrics).summary(SEARCH_RESPONSE_SIZE);
        verify(mockDistributionSummary).record(-1L);

        reset(mockDistributionSummary);

        // Test null body
        HttpHeaders headers = new HttpHeaders();
        headers.setContentLength(2000L);
        ResponseEntity<java.util.List<java.util.Map<String, Object>>> responseWithNullBody =
                new ResponseEntity<>(null, headers, HttpStatus.OK);

        MetricsHelper.publishSearchResponseSizeMetricInBytes(pluginMetrics, responseWithNullBody);
        verify(mockDistributionSummary).record(-1L);

        reset(mockDistributionSummary);

        // Test GET request with null
        MetricsHelper.publishGetResponseSizeMetricInBytes(pluginMetrics, (ResponseEntity<?>) null);
        verify(pluginMetrics).summary(GET_RESPONSE_SIZE);
        verify(mockDistributionSummary).record(-1L);
    }

    @Test
    void testGenericResponseEntityCompatibilityWithVariousTypes() {
        // Test that the generic ResponseEntity<?> works with various response types

        reset(pluginMetrics, mockDistributionSummary);
        when(pluginMetrics.summary(anyString())).thenReturn(mockDistributionSummary);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentLength(4000L);

        // Test with different generic types to ensure wildcard compatibility
        ResponseEntity<String> stringResponse = new ResponseEntity<>("test", headers, HttpStatus.OK);
        ResponseEntity<Integer> integerResponse = new ResponseEntity<>(42, headers, HttpStatus.OK);
        ResponseEntity<java.util.Map<String, String>> mapResponse =
                new ResponseEntity<>(java.util.Map.of("key", "value"), headers, HttpStatus.OK);

        // All should work with the generic ResponseEntity<?> overloads
        assertDoesNotThrow(() -> {
            MetricsHelper.publishSearchResponseSizeMetricInBytes(pluginMetrics, stringResponse);
            MetricsHelper.publishSearchResponseSizeMetricInBytes(pluginMetrics, integerResponse);
            MetricsHelper.publishSearchResponseSizeMetricInBytes(pluginMetrics, mapResponse);

            MetricsHelper.publishGetResponseSizeMetricInBytes(pluginMetrics, stringResponse);
            MetricsHelper.publishGetResponseSizeMetricInBytes(pluginMetrics, integerResponse);
            MetricsHelper.publishGetResponseSizeMetricInBytes(pluginMetrics, mapResponse);
        });

        // Verify all calls recorded the same Content-Length header value
        verify(mockDistributionSummary, times(6)).record(4000L);
    }

    // Functional tests demonstrating proper usage of the provided metrics
    // Note: These tests supersede basic provider tests as they verify both creation AND functionality

    @Test
    void testLogsRequestedCounterFunctionality() {
        Counter counter = MetricsHelper.provideLogsRequestedCounter(pluginMetrics);

        // Test increment functionality
        counter.increment();
        counter.increment(5);

        verify(mockCounter).increment();
        verify(mockCounter).increment(5);

        // Verify metric name
        verify(pluginMetrics).counter(LOGS_REQUESTED);
    }

    @Test
    void testGetRequestLatencyTimerFunctionality() {
        Timer timer = MetricsHelper.provideGetRequestLatencyTimer(pluginMetrics);

        // Test timer.record() with supplier (common usage pattern)
        String result = timer.record(() -> "test-result");

        verify(mockTimer).record(any(java.util.function.Supplier.class));

        // Verify metric name
        verify(pluginMetrics).timer(GET_REQUEST_LATENCY);
    }

    @Test
    void testSearchCallLatencyTimerFunctionality() {
        Timer timer = MetricsHelper.provideSearchCallLatencyTimer(pluginMetrics);

        // Test timer.record() with supplier (common usage pattern)
        String result = timer.record(() -> "search-result");

        verify(mockTimer).record(any(java.util.function.Supplier.class));

        // Verify metric name
        verify(pluginMetrics).timer(SEARCH_CALL_LATENCY);
    }

    @ParameterizedTest(name = "Functional test for {0} provider method")
    @MethodSource("functionalTestMethods")
    void testProviderMethodsFunctionalUsage(String methodType, String expectedMetricName) {
        if ("counter".equals(methodType)) {
            Counter counter = MetricsHelper.provideLogsRequestedCounter(pluginMetrics);

            // Demonstrate typical usage patterns
            counter.increment();
            counter.increment(3);

            verify(mockCounter).increment();
            verify(mockCounter).increment(3);
            verify(pluginMetrics).counter(expectedMetricName);

        } else { // timer
            Timer timer = "getRequestLatency".equals(expectedMetricName) ?
                    MetricsHelper.provideGetRequestLatencyTimer(pluginMetrics) :
                    MetricsHelper.provideSearchCallLatencyTimer(pluginMetrics);

            // Demonstrate typical usage patterns
            timer.record(() -> "operation-result");
            timer.record(java.time.Duration.ofMillis(100));

            verify(mockTimer).record(any(java.util.function.Supplier.class));
            verify(mockTimer).record(java.time.Duration.ofMillis(100));
            verify(pluginMetrics).timer(expectedMetricName);
        }
    }

    static Stream<Arguments> functionalTestMethods() {
        return Stream.of(
                Arguments.of("counter", LOGS_REQUESTED),
                Arguments.of("timer", GET_REQUEST_LATENCY),
                Arguments.of("timer", SEARCH_CALL_LATENCY)
        );
    }

    @Test
    void testProvideApiRequestsCounterFunctionality() {
        Counter counter = MetricsHelper.provideApiRequestsCounter(pluginMetrics);

        // Test increment functionality
        counter.increment();
        counter.increment(3);

        verify(mockCounter).increment();
        verify(mockCounter).increment(3);

        // Verify metric name
        verify(pluginMetrics).counter(TOTAL_API_REQUESTS);
    }

    @Test
    void testUsagePatternExamples() {
        // Demonstrate realistic usage patterns that would be used in Office365RestClient

        // Counter usage examples
        Counter logsCounter = MetricsHelper.provideLogsRequestedCounter(pluginMetrics);
        logsCounter.increment(); // Increment when a log is requested
        
        Counter apiCounter = MetricsHelper.provideApiRequestsCounter(pluginMetrics);
        apiCounter.increment(); // Increment for each API call made

        // Timer usage examples
        Timer getTimer = MetricsHelper.provideGetRequestLatencyTimer(pluginMetrics);
        Timer searchTimer = MetricsHelper.provideSearchCallLatencyTimer(pluginMetrics);

        // Typical timer usage with lambda (measuring operation duration)
        String getResult = getTimer.record(() -> {
            // Simulate GET operation
            return "audit-log-content";
        });

        String searchResult = searchTimer.record(() -> {
            // Simulate search operation
            return "search-results";
        });

        // Verify all interactions
        verify(mockCounter, times(2)).increment(); // Two counter increments
        verify(mockTimer, times(2)).record(any(java.util.function.Supplier.class));

        // Verify correct metric names were used
        verify(pluginMetrics).counter(LOGS_REQUESTED);
        verify(pluginMetrics).counter(TOTAL_API_REQUESTS);
        verify(pluginMetrics).timer(GET_REQUEST_LATENCY);
        verify(pluginMetrics).timer(SEARCH_CALL_LATENCY);
    }
}
