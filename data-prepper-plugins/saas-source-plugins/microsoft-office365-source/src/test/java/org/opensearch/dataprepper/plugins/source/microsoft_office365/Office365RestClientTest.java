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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.Office365AuthenticationProvider;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.models.AuditLogsResponse;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.microsoft_office365.utils.Constants.CONTENT_TYPES;

@ExtendWith(MockitoExtension.class)
class Office365RestClientTest {
    @Mock
    private RestTemplate restTemplate;
    @Mock
    private Office365AuthenticationProvider authConfig;

    private final PluginMetrics pluginMetrics = PluginMetrics.fromNames("Office365RestClientTest", "office365");

    private Office365RestClient office365RestClient;

    @BeforeEach
    void setUp() {
        office365RestClient = new Office365RestClient(authConfig, pluginMetrics);
        ReflectionTestUtils.setField(office365RestClient, "restTemplate", restTemplate);
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

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> office365RestClient.startSubscriptions());
        assertEquals("Failed to initialize subscriptions: 400 Bad Request",
                exception.getMessage());
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
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> office365RestClient.searchAuditLogs(
                        "Audit.AzureActiveDirectory",
                        startTime,
                        endTime,
                        null
                ));
        assertEquals("Failed to fetch audit logs", exception.getMessage());
    }

    @Test
    void testGetAuditLog() {
        String mockAuditLog = "{\"id\":\"123\",\"contentType\":\"Audit.AzureActiveDirectory\"}";
        ResponseEntity<String> mockResponse = new ResponseEntity<>(mockAuditLog, HttpStatus.OK);

        when(restTemplate.exchange(
                anyString(),
                eq(HttpMethod.GET),
                any(),
                eq(String.class)
        )).thenReturn(mockResponse);

        String result = office365RestClient.getAuditLog("test-content-id");

        assertEquals(mockAuditLog, result);
    }

    @Test
    void testGetAuditLogFailure() {
        // Mock REST template to throw an exception
        when(restTemplate.exchange(
                anyString(),
                eq(HttpMethod.GET),
                any(),
                eq(String.class)
        )).thenThrow(new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR));

        // Verify that the exception is propagated
        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> office365RestClient.getAuditLog("test-content-id"));
        assertEquals("Failed to fetch audit log", exception.getMessage());
    }
}

