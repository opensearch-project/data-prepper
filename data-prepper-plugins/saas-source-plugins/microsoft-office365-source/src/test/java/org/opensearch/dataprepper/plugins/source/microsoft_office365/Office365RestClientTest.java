/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.Office365AuthenticationInterface;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.models.AuditLogsResponse;
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.SaaSCrawlerException;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.microsoft_office365.utils.Constants.CONTENT_TYPES;

@ExtendWith(MockitoExtension.class)
class Office365RestClientTest {
    @Mock
    private RestTemplate restTemplate;
    @Mock
    private Office365AuthenticationInterface authConfig;

    private final PluginMetrics pluginMetrics = PluginMetrics.fromNames("Office365RestClientTest", "microsoft-office365");

    private Office365RestClient office365RestClient;

    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException{
        office365RestClient = new Office365RestClient(authConfig, pluginMetrics);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "restTemplate", restTemplate);
    }

    @Test
    void testStartSubscriptionsSuccess() {
        ResponseEntity<String> mockResponse = new ResponseEntity<>("{\"status\":\"enabled\"}", HttpStatus.OK);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), any(), eq(String.class)))
                .thenReturn(mockResponse);

        assertDoesNotThrow(() -> office365RestClient.startSubscriptions());
        verify(restTemplate, times(CONTENT_TYPES.length)).exchange(
                anyString(),
                eq(HttpMethod.POST),
                any(),
                eq(String.class)
        );
    }

    @Test
    void testStartSubscriptionsAlreadyEnabled() {
        HttpClientErrorException af20024Exception = new HttpClientErrorException(
                HttpStatus.BAD_REQUEST,
                "Bad Request",
                "{\"error\":\"AF20024\"}".getBytes(),
                null
        );
        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), any(), eq(String.class)))
                .thenThrow(af20024Exception);

        // Should not throw exception for AF20024
        assertDoesNotThrow(() -> office365RestClient.startSubscriptions());
    }

    @Test
    void testStartSubscriptionsOtherError() {
        HttpClientErrorException otherException = new HttpClientErrorException(
                HttpStatus.BAD_REQUEST,
                "Bad Request",
                "{\"error\":\"other_error\"}".getBytes(),
                null
        );
        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), any(), eq(String.class)))
                .thenThrow(otherException);

        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
                () -> office365RestClient.startSubscriptions());
        assertEquals("Failed to initialize subscriptions: 400 Bad Request",
                exception.getMessage());
        assertTrue(exception.isRetryable());
    }

    @Test
    void testSearchAuditLogs() {
        Instant startTime = Instant.now().minus(1, ChronoUnit.HOURS);
        Instant endTime = Instant.now();

        // Mock auth responses
        when(authConfig.getTenantId()).thenReturn("test-tenant-id");
        when(authConfig.getAccessToken()).thenReturn("test-access-token");

        // Create argument captors to capture what values are actually used in the API call
        ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<HttpEntity<?>> entityCaptor = ArgumentCaptor.forClass(HttpEntity.class);

        // Setup mock response
        List<Map<String, Object>> mockResults = Collections.singletonList(new HashMap<>());
        ResponseEntity<List<Map<String, Object>>> mockResponse = new ResponseEntity<>(mockResults, HttpStatus.OK);

        when(restTemplate.exchange(
                urlCaptor.capture(),
                eq(HttpMethod.GET),
                entityCaptor.capture(),
                any(ParameterizedTypeReference.class)
        )).thenReturn(mockResponse);

        office365RestClient.searchAuditLogs(
                "Audit.AzureActiveDirectory",
                startTime,
                endTime,
                null
        );

        // Verify URL construction
        String capturedUrl = urlCaptor.getValue();
        assertTrue(capturedUrl.contains("test-tenant-id"));
        assertTrue(capturedUrl.contains("/activity/feed/subscriptions/content"));
        assertTrue(capturedUrl.contains("contentType=Audit.AzureActiveDirectory"));
        assertTrue(capturedUrl.contains("startTime=" + startTime.toString()));
        assertTrue(capturedUrl.contains("endTime=" + endTime.toString()));

        // Verify headers contain correct access token
        HttpHeaders headers = entityCaptor.getValue().getHeaders();
        assertEquals("Bearer test-access-token", headers.getFirst("Authorization"));
    }

    @Test
    void testSearchAuditLogsWithPagination() {
        // Test with pageUri provided
        String pageUri = "https://next-page-url";

        // Setup mock response with NextPageUri header
        HttpHeaders headers = new HttpHeaders();
        headers.add("NextPageUri", "https://another-page");
        ResponseEntity<List<Map<String, Object>>> mockResponse = new ResponseEntity<>(
                Collections.singletonList(new HashMap<>()),
                headers,
                HttpStatus.OK
        );

        when(restTemplate.exchange(
                eq(pageUri),  // Should use provided pageUri directly
                eq(HttpMethod.GET),
                any(),
                any(ParameterizedTypeReference.class)
        )).thenReturn(mockResponse);

        AuditLogsResponse response = office365RestClient.searchAuditLogs(
                "Audit.AzureActiveDirectory",
                Instant.now(),
                Instant.now(),
                pageUri
        );

        assertEquals("https://another-page", response.getNextPageUri());
    }

    @Test
    void testSearchAuditLogsFailure() {
        Instant startTime = Instant.now().minus(1, ChronoUnit.HOURS);
        Instant endTime = Instant.now();

        // Mock REST template to throw an error
        when(restTemplate.exchange(
                anyString(),
                eq(HttpMethod.GET),
                any(),
                any(ParameterizedTypeReference.class)
        )).thenThrow(new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR));

        // Verify that the exception is propagated
        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
                () -> office365RestClient.searchAuditLogs(
                        "Audit.AzureActiveDirectory",
                        startTime,
                        endTime,
                        null
                ));
        assertEquals("Failed to fetch audit logs", exception.getMessage());
        assertTrue(exception.isRetryable());
    }

    @Test
    void testGetAuditLog() {
        String contentUri = "https://manage.office.com/api/v1.0/test-tenant/activity/feed/audit/123";
        String mockAuditLog = "{\"id\":\"123\",\"contentType\":\"Audit.AzureActiveDirectory\"}";
        ResponseEntity<String> mockResponse = new ResponseEntity<>(mockAuditLog, HttpStatus.OK);

        when(restTemplate.exchange(
                eq(contentUri),
                eq(HttpMethod.GET),
                any(),
                eq(String.class)
        )).thenReturn(mockResponse);

        String result = office365RestClient.getAuditLog(contentUri);

        assertEquals(mockAuditLog, result);
    }

    @Test
    void testGetAuditLogFailure() {
        String contentUri = "https://manage.office.com/api/v1.0/test-tenant/activity/feed/audit/123";
        // Mock REST template to throw an exception
        when(restTemplate.exchange(
                eq(contentUri),
                eq(HttpMethod.GET),
                any(),
                eq(String.class)
        )).thenThrow(new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR));

        // Verify that the exception is propagated
        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
                () -> office365RestClient.getAuditLog(contentUri));
        assertEquals("Failed to fetch audit log", exception.getMessage());
        assertTrue(exception.isRetryable());
    }

    @Test
    void testTokenRenewal() {
        // Setup
        Instant startTime = Instant.now().minus(1, ChronoUnit.HOURS);
        Instant endTime = Instant.now();

        List<String> tokensUsed = new ArrayList<>();
        List<String> requestTokens = new ArrayList<>();

        when(authConfig.getTenantId()).thenReturn("test-tenant-id");
        when(authConfig.getAccessToken()).thenAnswer(invocation -> {
            String token = "token-" + tokensUsed.size();
            tokensUsed.add(token);
            return token;
        });

        when(restTemplate.exchange(
                anyString(),
                eq(HttpMethod.GET),
                argThat(request -> {
                    requestTokens.add(request.getHeaders().getFirst("Authorization"));
                    return true;
                }),
                any(ParameterizedTypeReference.class)
        )).thenAnswer(invocation -> {
            if (requestTokens.size() == 1) {
                throw new HttpClientErrorException(HttpStatus.UNAUTHORIZED); // First request fails with 401
            }
            return new ResponseEntity<>(Collections.singletonList(new HashMap<>()), HttpStatus.OK); // Second request succeeds
        });

        // Execute
        office365RestClient.searchAuditLogs(
                "Audit.AzureActiveDirectory",
                startTime,
                endTime,
                null
        );

        // Verify
        assertEquals(2, requestTokens.size(), "Should have made two requests");
        assertEquals("Bearer token-0", requestTokens.get(0), "First request should use token-0");
        assertEquals("Bearer token-1", requestTokens.get(1), "Second request should use token-1");
    }

    @Test
    void testSearchAuditLogsFailureCounterIncrementsOnEachRetry() throws Exception {
        Instant startTime = Instant.now().minus(1, ChronoUnit.HOURS);
        Instant endTime = Instant.now();
        
        when(authConfig.getTenantId()).thenReturn("test-tenant-id");
        when(authConfig.getAccessToken()).thenReturn("test-access-token");

        Counter mockCounter = org.mockito.Mockito.mock(Counter.class);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "searchRequestsFailedCounter", mockCounter);

        when(restTemplate.exchange(
                anyString(),
                eq(HttpMethod.GET),
                any(),
                any(ParameterizedTypeReference.class)
        )).thenThrow(new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS));

        assertThrows(SaaSCrawlerException.class, () -> 
            office365RestClient.searchAuditLogs(
                    "Audit.AzureActiveDirectory",
                    startTime,
                    endTime,
                    null
            )
        );

        // Verify counter.increment() was called exactly 6 times (once for each retry attempt)
        verify(mockCounter, times(6)).increment();
    }

    @Test
    void testGetAuditLogFailureCounterIncrementsOnEachRetry() throws Exception {
        String contentUri = "https://manage.office.com/api/v1.0/test-tenant/activity/feed/audit/123";
        
        when(authConfig.getAccessToken()).thenReturn("test-access-token");

        Counter mockCounter = org.mockito.Mockito.mock(Counter.class);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "auditLogRequestsFailedCounter", mockCounter);

        when(restTemplate.exchange(
                eq(contentUri),
                eq(HttpMethod.GET),
                any(),
                eq(String.class)
        )).thenThrow(new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS));

        assertThrows(SaaSCrawlerException.class, () -> 
            office365RestClient.getAuditLog(contentUri)
        );

        // Verify counter.increment() was called exactly 6 times (once for each retry attempt)
        verify(mockCounter, times(6)).increment();
    }

    @Test
    void testMetricsInitialization() {
        // Test metrics initialization during construction. This approach is used for metrics that are called
        // inside RetryHandler.executeWithRetry() static method calls, which would require complex static mocking
        // to test for invocation. Testing initialization ensures the metrics infrastructure is properly set up.

        // Mock all required timers and counters for Office365RestClient constructor
        PluginMetrics mockPluginMetrics = org.mockito.Mockito.mock(PluginMetrics.class);
        Timer mockAuditLogFetchLatencyTimer = org.mockito.Mockito.mock(Timer.class);
        Timer mockSearchCallLatencyTimer = org.mockito.Mockito.mock(Timer.class);
        Counter mockAuditLogsRequestedCounter = org.mockito.Mockito.mock(Counter.class);
        Counter mockAuditLogRequestsFailedCounter = org.mockito.Mockito.mock(Counter.class);
        Counter mockAuditLogRequestsSuccessCounter = org.mockito.Mockito.mock(Counter.class);
        Counter mockSearchRequestsFailedCounter = org.mockito.Mockito.mock(Counter.class);
        Counter mockSearchRequestsSuccessCounter = org.mockito.Mockito.mock(Counter.class);
        Counter mockApiCallsCounter = org.mockito.Mockito.mock(Counter.class);
        DistributionSummary mockAuditLogRequestSizeSummary = org.mockito.Mockito.mock(DistributionSummary.class);
        DistributionSummary mockSearchRequestSizeSummary = org.mockito.Mockito.mock(DistributionSummary.class);

        when(mockPluginMetrics.timer("auditLogFetchLatency")).thenReturn(mockAuditLogFetchLatencyTimer);
        when(mockPluginMetrics.timer("searchCallLatency")).thenReturn(mockSearchCallLatencyTimer);
        when(mockPluginMetrics.counter("auditLogsRequested")).thenReturn(mockAuditLogsRequestedCounter);
        when(mockPluginMetrics.counter("auditLogRequestsFailed")).thenReturn(mockAuditLogRequestsFailedCounter);
        when(mockPluginMetrics.counter("auditLogRequestsSuccess")).thenReturn(mockAuditLogRequestsSuccessCounter);
        when(mockPluginMetrics.counter("searchRequestsFailed")).thenReturn(mockSearchRequestsFailedCounter);
        when(mockPluginMetrics.counter("searchRequestsSuccess")).thenReturn(mockSearchRequestsSuccessCounter);
        when(mockPluginMetrics.counter("apiCalls")).thenReturn(mockApiCallsCounter);
        when(mockPluginMetrics.summary("auditLogResponseSizeBytes")).thenReturn(mockAuditLogRequestSizeSummary);
        when(mockPluginMetrics.summary("searchResponseSizeBytes")).thenReturn(mockSearchRequestSizeSummary);

        // Create Office365RestClient with mocked metrics
        Office365RestClient testClient = new Office365RestClient(authConfig, mockPluginMetrics);

        // Verify all metrics were requested during construction
        verify(mockPluginMetrics).timer("auditLogFetchLatency");
        verify(mockPluginMetrics).timer("searchCallLatency");
        verify(mockPluginMetrics).counter("auditLogsRequested");
        verify(mockPluginMetrics).counter("auditLogRequestsFailed");
        verify(mockPluginMetrics).counter("auditLogRequestsSuccess");
        verify(mockPluginMetrics).counter("searchRequestsFailed");
        verify(mockPluginMetrics).counter("searchRequestsSuccess");
        verify(mockPluginMetrics).counter("apiCalls");
        verify(mockPluginMetrics).summary("auditLogResponseSizeBytes");
        verify(mockPluginMetrics).summary("searchResponseSizeBytes");
    }

    @Test
    void testGetAuditLogMetricsInvocation() throws NoSuchFieldException, IllegalAccessException {
        // Test metrics for getAuditLog() method - both success and failure scenarios

        // Create mock metrics and inject them
        Counter mockAuditLogsRequestedCounter = org.mockito.Mockito.mock(Counter.class);
        Counter mockAuditLogRequestsFailedCounter = org.mockito.Mockito.mock(Counter.class);
        Timer mockAuditLogFetchLatencyTimer = org.mockito.Mockito.mock(Timer.class);
        DistributionSummary mockAuditLogRequestSizeSummary = org.mockito.Mockito.mock(DistributionSummary.class);

        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "auditLogsRequestedCounter", mockAuditLogsRequestedCounter);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "auditLogRequestsFailedCounter", mockAuditLogRequestsFailedCounter);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "auditLogFetchLatencyTimer", mockAuditLogFetchLatencyTimer);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "auditLogResponseSizeSummary", mockAuditLogRequestSizeSummary);

        // Mock timer.record() to execute the lambda
        when(mockAuditLogFetchLatencyTimer.record(any(java.util.function.Supplier.class))).thenAnswer(invocation -> {
            java.util.function.Supplier<?> supplier = invocation.getArgument(0);
            return supplier.get();
        });

        String contentUri = "https://manage.office.com/api/v1.0/test-tenant/activity/feed/audit/123";

        // Test success scenario
        String mockAuditLog = "{\"id\":\"123\",\"contentType\":\"Audit.AzureActiveDirectory\"}";
        ResponseEntity<String> mockResponse = new ResponseEntity<>(mockAuditLog, HttpStatus.OK);
        when(restTemplate.exchange(eq(contentUri), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenReturn(mockResponse);

        office365RestClient.getAuditLog(contentUri);

        // Verify success metrics
        verify(mockAuditLogsRequestedCounter).increment(); // Called directly before RetryHandler
        verify(mockAuditLogFetchLatencyTimer).record(any(java.util.function.Supplier.class)); // Timer wrapper
        verify(mockAuditLogRequestSizeSummary).record(mockAuditLog.getBytes(java.nio.charset.StandardCharsets.UTF_8).length); // Size metric inside RetryHandler

        // Test failure scenario
        when(restTemplate.exchange(eq(contentUri), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR));

        assertThrows(SaaSCrawlerException.class, () -> office365RestClient.getAuditLog(contentUri));

        // Verify failure metrics
        verify(mockAuditLogsRequestedCounter, times(2)).increment(); // Called again before retry
        verify(mockAuditLogRequestsFailedCounter, times(6)).increment(); // Called 6 times (once for each retry attempt)
    }

    @Test
    void testSearchAuditLogsMetricsInvocation() throws NoSuchFieldException, IllegalAccessException {
        // Test metrics for searchAuditLogs() method - both success and failure scenarios

        // Create mock metrics and inject them
        Counter mockSearchRequestsFailedCounter = org.mockito.Mockito.mock(Counter.class);
        Timer mockSearchCallLatencyTimer = org.mockito.Mockito.mock(Timer.class);
        DistributionSummary mockSearchRequestSizeSummary = org.mockito.Mockito.mock(DistributionSummary.class);

        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "searchRequestsFailedCounter", mockSearchRequestsFailedCounter);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "searchCallLatencyTimer", mockSearchCallLatencyTimer);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "searchResponseSizeSummary", mockSearchRequestSizeSummary);

        // Mock timer.record() to execute the lambda
        when(mockSearchCallLatencyTimer.record(any(java.util.function.Supplier.class))).thenAnswer(invocation -> {
            java.util.function.Supplier<?> supplier = invocation.getArgument(0);
            return supplier.get();
        });

        // Test success scenario
        List<Map<String, Object>> mockResults = Collections.singletonList(new HashMap<>());
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setContentLength(1024L); // Mock content length
        ResponseEntity<List<Map<String, Object>>> mockResponse = new ResponseEntity<>(mockResults, responseHeaders, HttpStatus.OK);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.GET), any(), any(ParameterizedTypeReference.class)))
                .thenReturn(mockResponse);

        office365RestClient.searchAuditLogs(
                "Audit.AzureActiveDirectory",
                Instant.now().minus(1, ChronoUnit.HOURS),
                Instant.now(),
                null
        );

        // Verify success metrics
        verify(mockSearchCallLatencyTimer).record(any(java.util.function.Supplier.class)); // Timer wrapper
        verify(mockSearchRequestSizeSummary).record(1024L); // Size metric inside RetryHandler

        // Test failure scenario
        when(restTemplate.exchange(anyString(), eq(HttpMethod.GET), any(), any(ParameterizedTypeReference.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR));

        assertThrows(RuntimeException.class, () -> office365RestClient.searchAuditLogs(
                "Audit.AzureActiveDirectory",
                Instant.now().minus(1, ChronoUnit.HOURS),
                Instant.now(),
                null
        ));

        // Verify failure metrics
        verify(mockSearchRequestsFailedCounter, times(6)).increment(); // Called 6 times (once for each retry attempt)
    }

    @ParameterizedTest
    @CsvSource({
        "FORBIDDEN, true",
        "UNAUTHORIZED, true",
        "TOO_MANY_REQUESTS, true",
        "NOT_FOUND, true",
        "INTERNAL_SERVER_ERROR, false",
        "BAD_GATEWAY, false"
    })
    void testPublishErrorTypeMetricCounterForGetAuditLog(HttpStatus status, boolean shouldIncrementCounter) throws NoSuchFieldException, IllegalAccessException {
        // Mock error type counter map
        Map<String, Counter> mockErrorTypeMetricCounterMap = new HashMap<>();
        Counter mockCounter = org.mockito.Mockito.mock(Counter.class);
        mockErrorTypeMetricCounterMap.put(HttpStatus.FORBIDDEN.getReasonPhrase(), mockCounter);
        mockErrorTypeMetricCounterMap.put(HttpStatus.UNAUTHORIZED.getReasonPhrase(), mockCounter);
        mockErrorTypeMetricCounterMap.put(HttpStatus.TOO_MANY_REQUESTS.getReasonPhrase(), mockCounter);
        mockErrorTypeMetricCounterMap.put(HttpStatus.NOT_FOUND.getReasonPhrase(), mockCounter);

        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, 
                "errorTypeMetricCounterMap", mockErrorTypeMetricCounterMap);

        // Mock REST call to throw FORBIDDEN error
        String contentUri = "https://manage.office.com/api/v1.0/test-tenant/activity/feed/audit/123";
        when(restTemplate.exchange(
                eq(contentUri),
                eq(HttpMethod.GET),
                any(),
                eq(String.class)
        )).thenThrow(new HttpClientErrorException(status));

        // Execute and verify exception
        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class, 
                () -> office365RestClient.getAuditLog(contentUri));
        assertEquals("Failed to fetch audit log", exception.getMessage());
        assertTrue(exception.isRetryable());

        // Verify counter increment
        if (shouldIncrementCounter) {
            verify(mockCounter).increment();
        } else {
            verify(mockCounter, never()).increment();
        }
    }

    @Test
    void testPublishErrorTypeMetricCounterWithUnmappedError() throws NoSuchFieldException, IllegalAccessException {
        // Mock error type counter map with no matching error type
        Map<String, Counter> mockErrorTypeMetricCounterMap = new HashMap<>();
        
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, 
                "errorTypeMetricCounterMap", mockErrorTypeMetricCounterMap);

        // Mock REST call to throw unmapped error
        String contentUri = "https://manage.office.com/api/v1.0/test-tenant/activity/feed/audit/123";
        when(restTemplate.exchange(
                eq(contentUri),
                eq(HttpMethod.GET),
                any(),
                eq(String.class)
        )).thenThrow(new HttpClientErrorException(HttpStatus.BAD_GATEWAY));

        // Execute and verify exception
        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class, 
                () -> office365RestClient.getAuditLog(contentUri));
        assertEquals("Failed to fetch audit log", exception.getMessage());
        assertTrue(exception.isRetryable());

        // Verify no counters were incremented
        assertTrue(mockErrorTypeMetricCounterMap.isEmpty());
    }

    @ParameterizedTest
    @CsvSource({
        "FORBIDDEN, true",
        "UNAUTHORIZED, true",
        "TOO_MANY_REQUESTS, true",
        "NOT_FOUND, true",
        "INTERNAL_SERVER_ERROR, false",
        "BAD_GATEWAY, false"
    })
    void testPublishErrorTypeMetricCounterForSearchAuditLogs(HttpStatus status, boolean shouldIncrementCounter) 
        throws NoSuchFieldException, IllegalAccessException {
        // Mock error type counter map
        Map<String, Counter> mockErrorTypeMetricCounterMap = new HashMap<>();
        Counter mockCounter = org.mockito.Mockito.mock(Counter.class);
        mockErrorTypeMetricCounterMap.put(HttpStatus.FORBIDDEN.getReasonPhrase(), mockCounter);
        mockErrorTypeMetricCounterMap.put(HttpStatus.UNAUTHORIZED.getReasonPhrase(), mockCounter);
        mockErrorTypeMetricCounterMap.put(HttpStatus.TOO_MANY_REQUESTS.getReasonPhrase(), mockCounter);
        mockErrorTypeMetricCounterMap.put(HttpStatus.NOT_FOUND.getReasonPhrase(), mockCounter);

        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient,
                "errorTypeMetricCounterMap", mockErrorTypeMetricCounterMap);

        // Set up test parameters
        Instant startTime = Instant.now().minus(1, ChronoUnit.HOURS);
        Instant endTime = Instant.now();
        String contentType = "Audit.AzureActiveDirectory";

        // Mock auth config
        when(authConfig.getTenantId()).thenReturn("test-tenant-id");
        when(authConfig.getAccessToken()).thenReturn("test-access-token");

        // Mock REST call to throw error
        when(restTemplate.exchange(
                anyString(),
                eq(HttpMethod.GET),
                any(),
                any(ParameterizedTypeReference.class)
        )).thenThrow(new HttpClientErrorException(status));

        // Execute and verify exception
        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
                () -> office365RestClient.searchAuditLogs(
                contentType,
                startTime,
                endTime,
                null
                ));
        assertEquals("Failed to fetch audit logs", exception.getMessage());
        assertTrue(exception.isRetryable());

        // Verify counter increment
        if (shouldIncrementCounter) {
            verify(mockCounter).increment();
        } else {
            verify(mockCounter, never()).increment();
        }
    }

    @ParameterizedTest
    @CsvSource({
        "FORBIDDEN, true",
        "UNAUTHORIZED, true",
        "TOO_MANY_REQUESTS, true",
        "NOT_FOUND, true",
        "INTERNAL_SERVER_ERROR, false",
        "BAD_GATEWAY, false"
    })
    void testPublishErrorTypeMetricCounterForStartSubscriptions(HttpStatus status, boolean shouldIncrementCounter) 
        throws NoSuchFieldException, IllegalAccessException {
        // Mock error type counter map
        Map<String, Counter> mockErrorTypeMetricCounterMap = new HashMap<>();
        Counter mockCounter = org.mockito.Mockito.mock(Counter.class);
        mockErrorTypeMetricCounterMap.put(HttpStatus.FORBIDDEN.getReasonPhrase(), mockCounter);
        mockErrorTypeMetricCounterMap.put(HttpStatus.UNAUTHORIZED.getReasonPhrase(), mockCounter);
        mockErrorTypeMetricCounterMap.put(HttpStatus.TOO_MANY_REQUESTS.getReasonPhrase(), mockCounter);
        mockErrorTypeMetricCounterMap.put(HttpStatus.NOT_FOUND.getReasonPhrase(), mockCounter);

        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient,
        "errorTypeMetricCounterMap", mockErrorTypeMetricCounterMap);

        // Mock auth config
        when(authConfig.getTenantId()).thenReturn("test-tenant-id");
        when(authConfig.getAccessToken()).thenReturn("test-access-token");

        // Mock REST call to throw error
        when(restTemplate.exchange(
        anyString(),
        eq(HttpMethod.POST),
        any(),
        eq(String.class)
        )).thenThrow(new HttpClientErrorException(status));

        // Execute and verify exception
        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
        () -> office365RestClient.startSubscriptions());
        if (status == HttpStatus.FORBIDDEN) {
            assertEquals("Failed to initialize subscriptions: Access forbidden: 403 FORBIDDEN", 
                exception.getMessage());
        } else {
            assertEquals("Failed to initialize subscriptions: " + status.toString(), 
                exception.getMessage());
        }
        assertTrue(exception.isRetryable());

        if (shouldIncrementCounter) {
            verify(mockCounter).increment();
        } else {
            verify(mockCounter, never()).increment();
        }
    }

    @Test
    void testApiCallsCounterIncrementForStartSubscriptions() throws NoSuchFieldException, IllegalAccessException {
        // Test that apiCallsCounter is incremented for each subscription start call
        Counter mockApiCallsCounter = org.mockito.Mockito.mock(Counter.class);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "apiCallsCounter", mockApiCallsCounter);

        // Mock auth config
        when(authConfig.getTenantId()).thenReturn("test-tenant-id");
        when(authConfig.getAccessToken()).thenReturn("test-access-token");

        // Mock successful response
        ResponseEntity<String> mockResponse = new ResponseEntity<>("{\"status\":\"enabled\"}", HttpStatus.OK);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), any(), eq(String.class)))
                .thenReturn(mockResponse);

        // Execute
        office365RestClient.startSubscriptions();

        // Verify apiCallsCounter was incremented once for each content type
        verify(mockApiCallsCounter, times(CONTENT_TYPES.length)).increment();
    }

    @Test
    void testApiCallsCounterIncrementForSearchAuditLogs() throws NoSuchFieldException, IllegalAccessException {
        // Test that apiCallsCounter is incremented for search audit logs call
        Counter mockApiCallsCounter = org.mockito.Mockito.mock(Counter.class);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "apiCallsCounter", mockApiCallsCounter);

        // Mock auth config
        when(authConfig.getTenantId()).thenReturn("test-tenant-id");
        when(authConfig.getAccessToken()).thenReturn("test-access-token");

        // Mock successful response
        List<Map<String, Object>> mockResults = Collections.singletonList(new HashMap<>());
        ResponseEntity<List<Map<String, Object>>> mockResponse = new ResponseEntity<>(mockResults, HttpStatus.OK);
        when(restTemplate.exchange(anyString(), eq(HttpMethod.GET), any(), any(ParameterizedTypeReference.class)))
                .thenReturn(mockResponse);

        // Execute
        office365RestClient.searchAuditLogs(
                "Audit.AzureActiveDirectory",
                Instant.now().minus(1, ChronoUnit.HOURS),
                Instant.now(),
                null
        );

        // Verify apiCallsCounter was incremented once
        verify(mockApiCallsCounter, times(1)).increment();
    }

    @Test
    void testApiCallsCounterIncrementForGetAuditLog() throws NoSuchFieldException, IllegalAccessException {
        // Test that apiCallsCounter is incremented for get audit log call
        Counter mockApiCallsCounter = org.mockito.Mockito.mock(Counter.class);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "apiCallsCounter", mockApiCallsCounter);

        // Mock auth config
        when(authConfig.getAccessToken()).thenReturn("test-access-token");

        // Mock successful response
        String contentUri = "https://manage.office.com/api/v1.0/test-tenant/activity/feed/audit/123";
        String mockAuditLog = "{\"id\":\"123\",\"contentType\":\"Audit.AzureActiveDirectory\"}";
        ResponseEntity<String> mockResponse = new ResponseEntity<>(mockAuditLog, HttpStatus.OK);
        when(restTemplate.exchange(eq(contentUri), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenReturn(mockResponse);

        // Execute
        office365RestClient.getAuditLog(contentUri);

        // Verify apiCallsCounter was incremented once
        verify(mockApiCallsCounter, times(1)).increment();
    }

    @Test
    void testApiCallsCounterIncrementOnRetries() throws NoSuchFieldException, IllegalAccessException {
        // Test that apiCallsCounter is incremented for each retry attempt
        Counter mockApiCallsCounter = org.mockito.Mockito.mock(Counter.class);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "apiCallsCounter", mockApiCallsCounter);

        // Mock auth config
        when(authConfig.getAccessToken()).thenReturn("test-access-token");

        // Mock failure response that will trigger retries
        String contentUri = "https://manage.office.com/api/v1.0/test-tenant/activity/feed/audit/123";
        when(restTemplate.exchange(eq(contentUri), eq(HttpMethod.GET), any(), eq(String.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS));

        // Execute and expect exception
        assertThrows(RuntimeException.class, () -> office365RestClient.getAuditLog(contentUri));

        // Verify apiCallsCounter was incremented 6 times (once for each retry attempt)
        verify(mockApiCallsCounter, times(6)).increment();
    }

    @Test
    void testApiCallsCounterIncrementForSearchAuditLogsWithRetries() throws NoSuchFieldException, IllegalAccessException {
        // Test that apiCallsCounter is incremented for each retry attempt in searchAuditLogs
        Counter mockApiCallsCounter = org.mockito.Mockito.mock(Counter.class);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "apiCallsCounter", mockApiCallsCounter);

        // Mock auth config
        when(authConfig.getTenantId()).thenReturn("test-tenant-id");
        when(authConfig.getAccessToken()).thenReturn("test-access-token");

        // Mock failure response that will trigger retries
        when(restTemplate.exchange(anyString(), eq(HttpMethod.GET), any(), any(ParameterizedTypeReference.class)))
                .thenThrow(new HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS));

        // Execute and expect exception
        assertThrows(RuntimeException.class, () -> office365RestClient.searchAuditLogs(
                "Audit.AzureActiveDirectory",
                Instant.now().minus(1, ChronoUnit.HOURS),
                Instant.now(),
                null
        ));

        // Verify apiCallsCounter was incremented 6 times (once for each retry attempt)
        verify(mockApiCallsCounter, times(6)).increment();
    }
}
