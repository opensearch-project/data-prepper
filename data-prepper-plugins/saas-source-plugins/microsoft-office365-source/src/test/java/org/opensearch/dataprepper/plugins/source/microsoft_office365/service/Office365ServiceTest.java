/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.Office365RestClient;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.Office365SourceConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.SaaSCrawlerException;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.models.AuditLogsResponse;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class Office365ServiceTest {
    @Mock
    private Office365RestClient office365RestClient;

    @Mock
    private Office365SourceConfig sourceConfig;

    private Office365Service office365Service;

    private final PluginMetrics pluginMetrics = PluginMetrics.fromNames("Office365ServiceTest", "office365");

    @BeforeEach
    void setUp() {
        office365Service = new Office365Service(sourceConfig, office365RestClient, pluginMetrics);
        lenient().when(sourceConfig.getLookBackDuration(any(Instant.class))).thenReturn(Instant.now().minus(Duration.ofDays(365)));
    }

    @Test
    void testServiceInitialization() {
        assertNotNull(office365Service);
        when(office365RestClient.getAuditLog(anyString())).thenReturn("test audit log");
        assertNotNull(office365Service.getAuditLog("test-id"));
    }

    @Test
    void testSearchAuditLogs() {
        Instant startTime = Instant.now().minus(Duration.ofHours(1));
        Instant endTime = Instant.now();
        String logType = "Exchange";

        List<Map<String, Object>> items = new ArrayList<>();
        items.add(createTestItem("id1", Instant.now()));
        AuditLogsResponse expectedResponse = new AuditLogsResponse(items, null);

        when(office365RestClient.searchAuditLogs(
                eq(logType),
                eq(startTime),
                eq(endTime),
                isNull()
        )).thenReturn(expectedResponse);

        AuditLogsResponse response = office365Service.searchAuditLogs(
                logType,
                startTime,
                endTime,
                null
        );

        assertNotNull(response);
        assertEquals(1, response.getItems().size());
        verify(office365RestClient).searchAuditLogs(logType, startTime, endTime, null);
    }

    @Test
    void testSearchAuditLogsWithPagination() {
        Instant startTime = Instant.now().minus(Duration.ofHours(1));
        Instant endTime = Instant.now();
        String logType = "Exchange";

        // First page response
        List<Map<String, Object>> items1 = new ArrayList<>();
        items1.add(createTestItem("id1", Instant.now()));
        AuditLogsResponse response1 = new AuditLogsResponse(items1, "nextPage");

        // Second page response
        List<Map<String, Object>> items2 = new ArrayList<>();
        items2.add(createTestItem("id2", Instant.now()));
        AuditLogsResponse response2 = new AuditLogsResponse(items2, null);

        when(office365RestClient.searchAuditLogs(
                eq(logType),
                eq(startTime),
                eq(endTime),
                isNull()
        )).thenReturn(response1);

        when(office365RestClient.searchAuditLogs(
                eq(logType),
                eq(startTime),
                eq(endTime),
                eq("nextPage")
        )).thenReturn(response2);

        // Execute first page
        AuditLogsResponse firstResponse = office365Service.searchAuditLogs(
                logType,
                startTime,
                endTime,
                null
        );

        // Execute second page
        AuditLogsResponse secondResponse = office365Service.searchAuditLogs(
                logType,
                startTime,
                endTime,
                firstResponse.getNextPageUri()
        );

        assertNotNull(firstResponse);
        assertEquals("nextPage", firstResponse.getNextPageUri());
        assertEquals(1, firstResponse.getItems().size());

        assertNotNull(secondResponse);
        assertNull(secondResponse.getNextPageUri());
        assertEquals(1, secondResponse.getItems().size());
    }

    @Test
    void testSearchAuditLogsWithEmptyResponse() {
        Instant startTime = Instant.now().minus(Duration.ofHours(1));
        Instant endTime = Instant.now();
        String logType = "Exchange";

        AuditLogsResponse emptyResponse = new AuditLogsResponse(new ArrayList<>(), null);

        when(office365RestClient.searchAuditLogs(
                eq(logType),
                eq(startTime),
                eq(endTime),
                isNull()
        )).thenReturn(emptyResponse);

        AuditLogsResponse response = office365Service.searchAuditLogs(
                logType,
                startTime,
                endTime,
                null
        );

        assertNotNull(response);
        assertEquals(0, response.getItems().size());
        assertNull(response.getNextPageUri());
    }

    @Test
    void testSearchAuditLogsError() {
        Instant startTime = Instant.now().minus(Duration.ofHours(1));
        Instant endTime = Instant.now();
        String logType = "Exchange";

        lenient().when(office365RestClient.searchAuditLogs(
                any(), any(), any(), any()
        )).thenThrow(new RuntimeException("API Error"));

        SaaSCrawlerException exception = assertThrows(
                SaaSCrawlerException.class,
                () -> office365Service.searchAuditLogs(logType, startTime, endTime, null)
        );

        assertEquals("Failed to fetch logs for time window " + startTime + " to " + endTime +
                " for log type " + logType + ".", exception.getMessage());
        assertTrue(exception.isRetryable());
    }

    @Test
    void testInitializeSubscriptions() {
        office365Service.initializeSubscriptions();
        verify(office365RestClient).startSubscriptions();
    }

    @Test
    void testGetAuditLog() {
        String expectedLog = "{\"test\": \"log\"}";
        when(office365RestClient.getAuditLog("test-id")).thenReturn(expectedLog);

        String actualLog = office365Service.getAuditLog("test-id");
        assertEquals(expectedLog, actualLog);
        verify(office365RestClient).getAuditLog("test-id");
    }

    @Test
    void testSearchAuditLogsWithNullStartTime() {
        // Given
        Instant endTime = Instant.now();
        String logType = "Exchange";

        // When/Then
        SaaSCrawlerException exception = assertThrows(
                SaaSCrawlerException.class,
                () -> office365Service.searchAuditLogs(logType, null, endTime, null)
        );
        assertEquals("startTime and endTime must not be null", exception.getMessage());
        assertFalse(exception.isRetryable());
    }

    @Test
    void testSearchAuditLogsWithNullEndTime() {
        // Given
        Instant startTime = Instant.now().minus(Duration.ofHours(1));
        String logType = "Exchange";

        // When/Then
        SaaSCrawlerException exception = assertThrows(
                SaaSCrawlerException.class,
                () -> office365Service.searchAuditLogs(logType, startTime, null, null)
        );
        assertEquals("startTime and endTime must not be null", exception.getMessage());
        assertFalse(exception.isRetryable());
     }

     @Test
     void testSearchAuditLogsWithBothTimesNull() {
        // Given
        String logType = "Exchange";

        // When/Then
        SaaSCrawlerException exception = assertThrows(
                SaaSCrawlerException.class,
                () -> office365Service.searchAuditLogs(logType, null, null, null)
        );
        assertEquals("startTime and endTime must not be null", exception.getMessage());
        assertFalse(exception.isRetryable());
     }

     @Test
     void testSearchAuditLogsWithNullLogType() {
        // Given
        Instant startTime = Instant.now().minus(Duration.ofHours(1));
        Instant endTime = Instant.now();

        // When/Then
        SaaSCrawlerException exception = assertThrows(
                SaaSCrawlerException.class,
                () -> office365Service.searchAuditLogs(null, startTime, endTime, null)
        );
        assertEquals("logType must not be null", exception.getMessage());
        assertFalse(exception.isRetryable());
     }

    @Test
    void testSearchAuditLogs_WithRange_AdjustsStartTime() {
        Clock fixedClock = Clock.fixed(Instant.parse("2025-11-09T21:30:00.00Z"), ZoneOffset.UTC);
        Instant now = Instant.now(fixedClock);

        // Create a time window that's beyond our configured range:
        // Start time = current time - (4 days + 30 minutes)
        // This simulates the crawler creating a partition that starts slightly beyond our range
        Instant startTime = now.minus(Duration.ofDays(4))
                .minus(Duration.ofMinutes(30));
        // End time = start time + 1 hour
        Instant endTime = startTime.plus(Duration.ofHours(1));

        String logType = "Exchange";
        long lookBackMinutes = 96 * 60; // Configure 4 days range limit
        Instant lookBackDuration = now.minus(Duration.ofMinutes(lookBackMinutes));
        when(sourceConfig.getLookBackDuration(any(Instant.class))).thenReturn(lookBackDuration);
        when(office365RestClient.searchAuditLogs(
                any(String.class),
                any(Instant.class),
                any(Instant.class),
                any()
        )).thenReturn(new AuditLogsResponse(new ArrayList<>(), null));

        office365Service.searchAuditLogs(logType, startTime, endTime, null);

        // Capture the actual start time that was passed to the REST client
        ArgumentCaptor<Instant> startTimeCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(office365RestClient).searchAuditLogs(
                eq(logType),
                startTimeCaptor.capture(),
                eq(endTime),
                isNull()
        );

        // Verify that the service adjusted the start time correctly:
        Instant capturedStartTime = startTimeCaptor.getValue();
        Duration actualLookback = Duration.between(capturedStartTime, now);
        assertEquals(96, actualLookback.toHours(),
                "Adjusted start time should be exactly 96 hours ago");
    }

    @Test
    void testSearchAuditLogs_WithSubHourRange_AdjustsStartTime() {
        Clock fixedClock = Clock.fixed(Instant.parse("2025-11-09T21:30:00.00Z"), ZoneOffset.UTC);
        Instant now = Instant.now(fixedClock);

        Instant startTime = now.minus(Duration.ofMinutes(45));
        Instant endTime = startTime.plus(Duration.ofMinutes(15));

        String logType = "Exchange";
        long lookBackMinutes = 30L;
        Instant lookBackDuration = now.minus(Duration.ofMinutes(lookBackMinutes));

        when(sourceConfig.getLookBackDuration(any(Instant.class))).thenReturn(lookBackDuration);
        when(office365RestClient.searchAuditLogs(
                any(String.class),
                any(Instant.class),
                any(Instant.class),
                any()
        )).thenReturn(new AuditLogsResponse(new ArrayList<>(), null));

        office365Service.searchAuditLogs(logType, startTime, endTime, null);

        // Capture the actual start time that was passed to the REST client
        ArgumentCaptor<Instant> startTimeCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(office365RestClient).searchAuditLogs(
                eq(logType),
                startTimeCaptor.capture(),
                eq(endTime),
                isNull()
        );

        // Verify that the service adjusted the start time correctly:
        Instant capturedStartTime = startTimeCaptor.getValue();
        Duration actualLookback = Duration.between(capturedStartTime, now);
        assertEquals(45, actualLookback.toMinutes(),
                "Adjusted start time should be exactly 30 minutes ago");
    }

    private Map<String, Object> createTestItem(String contentId, Instant contentCreated) {
        Map<String, Object> item = new HashMap<>();
        item.put("contentId", contentId);
        item.put("contentCreated", contentCreated.toString());
        item.put("contentUri", "https://test.com");
        return item;
    }
}
