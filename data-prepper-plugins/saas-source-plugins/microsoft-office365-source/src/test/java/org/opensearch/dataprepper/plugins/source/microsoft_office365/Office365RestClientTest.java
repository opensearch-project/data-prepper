/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.micrometer.core.instrument.Counter;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.Office365AuthenticationInterface;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.models.AuditLogsResponse;
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.SaaSCrawlerException;
import org.opensearch.dataprepper.plugins.source.source_crawler.metrics.VendorAPIMetricsRecorder;
import org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry.DefaultRetryStrategy;
import org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry.DefaultStatusCodeHandler;
import org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry.RetryHandler;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.microsoft_office365.utils.Constants.CONTENT_TYPES;

/**
 * Unit tests for Office365RestClient.
 * 
 * Tests REST API interactions with Office 365 Management API including:
 * - Subscription management (create, handle existing subscriptions)
 * - Audit log search and retrieval operations
 * - Error handling and retry logic
 * - Authentication and token renewal flows
 * - Pagination support for large result sets
 */
@ExtendWith(MockitoExtension.class)
class Office365RestClientTest {
    @Mock
    private RestTemplate restTemplate;
    @Mock
    private Office365AuthenticationInterface authConfig;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private VendorAPIMetricsRecorder metricsRecorder;
    @Mock
    private Counter apiCallsCounter;

    private Office365RestClient office365RestClient;

    private static final int CUSTOM_MAX_RETRIES = 1;

    @BeforeEach
    void setUp() throws NoSuchFieldException, IllegalAccessException {
        // Setup VendorAPIMetricsRecorder method mocks - use lenient to avoid unnecessary stubbing errors
        lenient().when(metricsRecorder.recordAuthLatency(any(java.util.function.Supplier.class))).thenAnswer(invocation -> invocation.getArgument(0, java.util.function.Supplier.class).get());
        lenient().when(metricsRecorder.recordSearchLatency(any(java.util.function.Supplier.class))).thenAnswer(invocation -> invocation.getArgument(0, java.util.function.Supplier.class).get());
        lenient().when(metricsRecorder.recordGetLatency(any(java.util.function.Supplier.class))).thenAnswer(invocation -> invocation.getArgument(0, java.util.function.Supplier.class).get());
        
        // Setup Runnable overload mocks - execute the runnable when called
        lenient().doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0, Runnable.class);
            runnable.run();
            return null;
        }).when(metricsRecorder).recordAuthLatency(any(Runnable.class));
        
        lenient().doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0, Runnable.class);
            runnable.run();
            return null;
        }).when(metricsRecorder).recordSearchLatency(any(Runnable.class));
        
        lenient().doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0, Runnable.class);
            runnable.run();
            return null;
        }).when(metricsRecorder).recordGetLatency(any(Runnable.class));
        
        // Void methods don't need stubbing - Mockito handles them automatically
        
        // Mock the counter creation
        when(pluginMetrics.counter("apiCalls")).thenReturn(apiCallsCounter);

        office365RestClient = new Office365RestClient(authConfig, pluginMetrics, metricsRecorder);
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "restTemplate", restTemplate);

        // Optionally replace RetryHandler with custom one (1 retry) for faster test execution
        RetryHandler customRetryHandler = new RetryHandler(
                new DefaultRetryStrategy(CUSTOM_MAX_RETRIES),
                new DefaultStatusCodeHandler());
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "retryHandler", customRetryHandler);
    }

    /**
     * Tests successful subscription creation for all Office 365 content types.
     * Verifies that POST requests are made for each content type and no exceptions are thrown.
     */
    @Test
    void testStartSubscriptionsSuccess() {
        when(authConfig.getTenantId()).thenReturn("test-tenant");
        when(authConfig.getAccessToken()).thenReturn("test-token");
        
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

    /**
     * Tests handling of AF20024 error code when subscriptions are already enabled.
     * Verifies that this specific error is treated as success and doesn't throw an exception.
     */
    @Test
    void testStartSubscriptionsAlreadyEnabled() {
        when(authConfig.getTenantId()).thenReturn("test-tenant");
        when(authConfig.getAccessToken()).thenReturn("test-token");
        
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

    /**
     * Tests error handling for subscription failures other than AF20024.
     * Verifies that non-AF20024 errors are propagated as retryable SaaSCrawlerException.
     */
    @Test
    void testStartSubscriptionsOtherError() {
        when(authConfig.getTenantId()).thenReturn("test-tenant");
        when(authConfig.getAccessToken()).thenReturn("test-token");
        
        HttpClientErrorException otherException = new HttpClientErrorException(
                HttpStatus.BAD_REQUEST,
                "Bad Request",
                "{\"error\":\"other_error\"}".getBytes(),
                null
        );
        when(restTemplate.exchange(anyString(), eq(HttpMethod.POST), any(), eq(String.class)))
                .thenThrow(otherException)
                .thenThrow(otherException)  // Retry will call this again
                .thenThrow(otherException); // Final retry

        SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
                () -> office365RestClient.startSubscriptions());
        assertTrue(exception.getMessage().contains("Failed to initialize subscriptions"));
        assertTrue(exception.isRetryable());
    }

    /**
     * Tests audit log search functionality with URL and header validation.
     * Verifies URL construction, authentication headers, and response mapping.
     */
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

        AuditLogsResponse result = office365RestClient.searchAuditLogs(
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
        
        // Verify result
        assertEquals(mockResults, result.getItems());
    }

    /**
     * Tests pagination support in audit log search.
     * Verifies that pageUri is used correctly and NextPageUri header is extracted from response.
     */
    @Test
    void testSearchAuditLogsWithPagination() {
        // Test with pageUri provided
        String pageUri = "https://next-page-url";

        when(authConfig.getAccessToken()).thenReturn("test-token");

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

    /**
     * Tests error handling for audit log search failures.
     * Verifies that persistent errors are propagated as retryable SaaSCrawlerException.
     */
    @Test
    void testSearchAuditLogsFailure() {
        Instant startTime = Instant.now().minus(1, ChronoUnit.HOURS);
        Instant endTime = Instant.now();

        when(authConfig.getTenantId()).thenReturn("test-tenant");
        when(authConfig.getAccessToken()).thenReturn("test-token");

        // Mock REST template to throw an error that persists through retries
        HttpClientErrorException exception = new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR);
        when(restTemplate.exchange(
                anyString(),
                eq(HttpMethod.GET),
                any(),
                any(ParameterizedTypeReference.class)
        )).thenThrow(exception)
          .thenThrow(exception)  // Retry 1
          .thenThrow(exception); // Retry 2

        // Verify that the exception is propagated
        SaaSCrawlerException crawlerException = assertThrows(SaaSCrawlerException.class,
                () -> office365RestClient.searchAuditLogs(
                        "Audit.AzureActiveDirectory",
                        startTime,
                        endTime,
                        null
                ));
        assertEquals("Failed to fetch audit logs", crawlerException.getMessage());
        assertTrue(crawlerException.isRetryable());
    }

    /**
     * Tests basic audit log retrieval functionality.
     * Verifies that audit log content can be fetched from a specific URI.
     */
    @Test
    void testGetAuditLog() {
        String contentUri = "https://manage.office.com/api/v1.0/test-tenant/activity/feed/audit/123";
        String mockAuditLog = "{\"id\":\"123\",\"contentType\":\"Audit.AzureActiveDirectory\"}";
        
        when(authConfig.getAccessToken()).thenReturn("test-token");
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

    /**
     * Tests error handling for audit log retrieval failures.
     * Verifies that persistent errors are propagated as retryable SaaSCrawlerException.
     */
    @Test
    void testGetAuditLogFailure() {
        String contentUri = "https://manage.office.com/api/v1.0/test-tenant/activity/feed/audit/123";
        
        when(authConfig.getAccessToken()).thenReturn("test-token");
        
        // Mock REST template to throw an exception that persists through retries
        HttpClientErrorException exception = new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR);
        when(restTemplate.exchange(
                eq(contentUri),
                eq(HttpMethod.GET),
                any(),
                eq(String.class)
        )).thenThrow(exception)
          .thenThrow(exception)  // Retry 1
          .thenThrow(exception); // Retry 2

        // Verify that the exception is propagated
        SaaSCrawlerException crawlerException = assertThrows(SaaSCrawlerException.class,
                () -> office365RestClient.getAuditLog(contentUri));
        assertEquals("Failed to fetch audit log", crawlerException.getMessage());
        assertTrue(crawlerException.isRetryable());
    }

    /**
     * Tests automatic token renewal when receiving 401 Unauthorized response.
     * Verifies that failed authentication triggers credential renewal and retry succeeds.
     */
    @Test
    void testTokenRenewal() throws NoSuchFieldException, IllegalAccessException {
        // Setup
        Instant startTime = Instant.now().minus(1, ChronoUnit.HOURS);
        Instant endTime = Instant.now();

        // Override retry handler to allow at least 2 attempts for token renewal test
        RetryHandler tokenRenewalRetryHandler = new RetryHandler(
                new DefaultRetryStrategy(2), // Need at least 2 attempts for token renewal scenario
                new DefaultStatusCodeHandler());
        ReflectivelySetField.setField(Office365RestClient.class, office365RestClient, "retryHandler",
                tokenRenewalRetryHandler);

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

}
