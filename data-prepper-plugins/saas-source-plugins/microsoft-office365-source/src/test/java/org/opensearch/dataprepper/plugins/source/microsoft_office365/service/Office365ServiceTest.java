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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.Office365RestClient;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.Office365SourceConfig;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.exception.Office365Exception;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.models.AuditLogsResponse;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
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

        when(office365RestClient.searchAuditLogs(
                any(), any(), any(), any()
        )).thenThrow(new RuntimeException("API Error"));

        Office365Exception exception = assertThrows(
                Office365Exception.class,
                () -> office365Service.searchAuditLogs(logType, startTime, endTime, null)
        );

        assertEquals("Failed to fetch logs for time window " + startTime + " to " + endTime +
                " for log type " + logType + ".", exception.getMessage());
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

    private Map<String, Object> createTestItem(String contentId, Instant contentCreated) {
        Map<String, Object> item = new HashMap<>();
        item.put("contentId", contentId);
        item.put("contentCreated", contentCreated.toString());
        item.put("contentUri", "https://test.com");
        return item;
    }
}
