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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class MetricsHelperTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter mockCounter;

    @Mock
    private DistributionSummary mockDistributionSummary;

    @BeforeEach
    void setUp() {
        lenient().when(pluginMetrics.counter(anyString())).thenReturn(mockCounter);
        lenient().when(pluginMetrics.summary(anyString())).thenReturn(mockDistributionSummary);
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
        verify(pluginMetrics, times(2)).counter("requestAccessDenied");
        verify(pluginMetrics).counter("requestThrottled");
        verify(pluginMetrics).counter("resourceNotFound");
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
                Arguments.of("search", "searchResponseSizeBytes",
                        (BiConsumer<PluginMetrics, ResponseEntity<?>>) MetricsHelper::publishSearchResponseSizeMetricInBytes,
                        (BiConsumer<PluginMetrics, String>) MetricsHelper::publishSearchResponseSizeMetricInBytes),
                Arguments.of("get", "getResponseSizeBytes",
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
                Arguments.of("search", "searchRequestsSuccess"),
                Arguments.of("get", "getRequestsSuccess")
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
                Arguments.of("search", "searchRequestsFailed"),
                Arguments.of("get", "getRequestsFailed")
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
        verify(pluginMetrics).summary("searchResponseSizeBytes");
        verify(pluginMetrics).summary("getResponseSizeBytes");

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

        verify(pluginMetrics).summary("searchResponseSizeBytes");
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

        verify(pluginMetrics).summary("getResponseSizeBytes");
        verify(mockDistributionSummary).record(1500L);
    }

    @Test
    void testGenericResponseEntityOverloadsWithNullValues() {
        // Test the new generic ResponseEntity<?> overloads with null scenarios

        reset(pluginMetrics, mockDistributionSummary);
        when(pluginMetrics.summary(anyString())).thenReturn(mockDistributionSummary);

        // Test null response
        MetricsHelper.publishSearchResponseSizeMetricInBytes(pluginMetrics, (ResponseEntity<?>) null);
        verify(pluginMetrics).summary("searchResponseSizeBytes");
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
        verify(pluginMetrics).summary("getResponseSizeBytes");
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
}
