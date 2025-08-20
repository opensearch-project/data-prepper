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
import org.opensearch.dataprepper.plugins.source.microsoft_office365.models.AuditLogsResponse;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.utils.Constants;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class Office365ServiceTest {
    @Mock
    private Office365RestClient office365RestClient;

    @Mock
    private Office365SourceConfig sourceConfig;

    private org.opensearch.dataprepper.plugins.source.microsoft_office365.service.Office365Service office365Service;
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
    void testGetOffice365EntitiesWithMultipleTimeWindows() {
        // Create test data
        Instant contentCreated = Instant.now().minusSeconds(100);
        List<Map<String, Object>> items1 = new ArrayList<>();
        List<Map<String, Object>> items2 = new ArrayList<>();
        items1.add(createTestItem("id1", contentCreated));
        items2.add(createTestItem("id2", contentCreated));

        AuditLogsResponse response1 = new AuditLogsResponse(items1, "nextPageUri1");
        AuditLogsResponse response2 = new AuditLogsResponse(items2, null);

        // Fix the time windows - use current time as reference
        Instant now = Instant.now();
        Instant startTime = now.minus(Duration.ofHours(3)); // Start 3 hours ago

        // Setup mock behavior - return response for any time range
        when(office365RestClient.searchAuditLogs(
                anyString(),
                any(Instant.class),
                any(Instant.class),
                isNull()))
                .thenReturn(response1);
        when(office365RestClient.searchAuditLogs(
                anyString(),
                any(Instant.class),
                any(Instant.class),
                eq("nextPageUri1")))
                .thenReturn(response2);

        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();

        office365Service.getOffice365Entities(startTime, itemInfoQueue);

        // Verify the API calls
        ArgumentCaptor<String> contentTypeCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Instant> startTimeCaptor = ArgumentCaptor.forClass(Instant.class);
        ArgumentCaptor<Instant> endTimeCaptor = ArgumentCaptor.forClass(Instant.class);

        verify(office365RestClient, atLeast(Constants.CONTENT_TYPES.length * 3))
                .searchAuditLogs(
                        contentTypeCaptor.capture(),
                        startTimeCaptor.capture(),
                        endTimeCaptor.capture(),
                        isNull());

        // Calculate the actual number of time windows
        long distinctTimeWindows = startTimeCaptor.getAllValues().stream()
                .distinct()
                .count();

        // Verify the number of items
        int expectedItems = Constants.CONTENT_TYPES.length * (int)distinctTimeWindows * 2;
        assertEquals(expectedItems, itemInfoQueue.size(),
                String.format("Expected %d items (%d content types * %d time windows * 1 item), but found %d",
                        expectedItems, Constants.CONTENT_TYPES.length, distinctTimeWindows, itemInfoQueue.size()));


        Instant expectedLastModifiedAtForSecondPage = contentCreated.plusMillis(1);
        for (int i = 0; i < Constants.CONTENT_TYPES.length * (int) distinctTimeWindows * 2; i++) {
            ItemInfo itemInfo = itemInfoQueue.poll();
            assertEquals(contentCreated, itemInfo.getEventTime());
            if (i % 2 == 0) {
                assertEquals("id1", itemInfo.getItemId());
                assertEquals(contentCreated, itemInfo.getLastModifiedAt(),
                        "Expect first page's lastModifiedAt timestamp to be same as contentCreated timestamp");
            } else {
                assertEquals("id2", itemInfo.getItemId());
                assertEquals(expectedLastModifiedAtForSecondPage, itemInfo.getLastModifiedAt(),
                        "Expect second page's lastModifiedAt timestamp to be 1ms after contentCreated timestamp");
            }
        }

        // Verify we have at least 3 distinct time windows
        assertTrue(distinctTimeWindows >= 3,
                "Expected at least 3 distinct time windows, but found " + distinctTimeWindows);

        // Verify all content types were used
        Set<String> usedContentTypes = new HashSet<>(contentTypeCaptor.getAllValues());
        assertEquals(Constants.CONTENT_TYPES.length, usedContentTypes.size(),
                "Not all content types were processed");

        // Verify that each time window is one hour long (or less for the last one)
        List<Instant> startTimes = startTimeCaptor.getAllValues();
        List<Instant> endTimes = endTimeCaptor.getAllValues();
        for (int i = 0; i < startTimes.size(); i++) {
            Duration windowDuration = Duration.between(startTimes.get(i), endTimes.get(i));
            assertTrue(windowDuration.compareTo(Duration.ofHours(1)) <= 0,
                    "Time window " + i + " is longer than one hour: " + windowDuration);
        }
    }

    @Test
    void testEmptyResponse() {
        AuditLogsResponse emptyResponse = new AuditLogsResponse(new ArrayList<>(), null);
        when(office365RestClient.searchAuditLogs(anyString(), any(Instant.class), any(Instant.class), any()))
                .thenReturn(emptyResponse);

        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();
        Instant timestamp = Instant.now().minus(Duration.ofHours(1));

        office365Service.getOffice365Entities(timestamp, itemInfoQueue);

        assertTrue(itemInfoQueue.isEmpty(), "Queue should be empty when receiving empty response");
    }

    @Test
    void testGetOffice365EntitiesWithSevenDayLimit() {
        List<Map<String, Object>> items = new ArrayList<>();
        Map<String, Object> item = createTestItem("id", Instant.now());
        items.add(item);

        AuditLogsResponse mockResponse = new AuditLogsResponse(items, null);
        when(office365RestClient.searchAuditLogs(
                anyString(),
                any(Instant.class),
                any(Instant.class),
                any()))
                .thenReturn(mockResponse);

        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();
        Instant now = Instant.now();
        Instant timestamp = now.minus(Duration.ofDays(10)); // Start 10 days ago

        office365Service.getOffice365Entities(timestamp, itemInfoQueue);

        // Verify that items were added to the queue
        assertFalse(itemInfoQueue.isEmpty(), "Queue should not be empty");

        // Verify that the REST client was called with adjusted time (7 days ago)
        ArgumentCaptor<Instant> startTimeCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(office365RestClient, atLeast(1))
                .searchAuditLogs(anyString(), startTimeCaptor.capture(), any(Instant.class), any());

        Instant firstStartTime = startTimeCaptor.getAllValues().get(0);
        Duration adjustedDuration = Duration.between(firstStartTime, now);
        assertTrue(adjustedDuration.toDays() <= 7,
                "Start time should be adjusted to 7 days ago, but was " + adjustedDuration.toDays() + " days");
    }

    @Test
    void testRetryBehaviorOnFailure() {
        Instant now = Instant.now();
        Instant startTime = now.minus(Duration.ofMinutes(10)); // Small window to ensure single time slice
        Queue<ItemInfo> itemInfoQueue = new ConcurrentLinkedQueue<>();

        // Create successful response
        List<Map<String, Object>> items = new ArrayList<>();
        items.add(createTestItem("id", Instant.now()));
        AuditLogsResponse successResponse = new AuditLogsResponse(items, null);

        // Set up mock to fail first then succeed for first content type
        AtomicInteger firstTypeCallCount = new AtomicInteger(0);
        when(office365RestClient.searchAuditLogs(
                eq(Constants.CONTENT_TYPES[0]),
                any(Instant.class),
                any(Instant.class),
                isNull()))
                .thenAnswer(invocation -> {
                    if (firstTypeCallCount.getAndIncrement() == 0) {
                        throw new RuntimeException("API Error");
                    }
                    return successResponse;
                });

        // Other content types succeed immediately
        for (int i = 1; i < Constants.CONTENT_TYPES.length; i++) {
            when(office365RestClient.searchAuditLogs(
                    eq(Constants.CONTENT_TYPES[i]),
                    any(Instant.class),
                    any(Instant.class),
                    isNull()))
                    .thenReturn(successResponse);
        }

        office365Service.getOffice365Entities(startTime, itemInfoQueue);

        // Verify that the first content type was called at least twice (one failure, one success)
        verify(office365RestClient, atLeast(2))
                .searchAuditLogs(
                        eq(Constants.CONTENT_TYPES[0]),
                        any(Instant.class),
                        any(Instant.class),
                        isNull());

        // Verify that other content types were called at least once
        for (int i = 1; i < Constants.CONTENT_TYPES.length; i++) {
            verify(office365RestClient, atLeast(1))
                    .searchAuditLogs(
                            eq(Constants.CONTENT_TYPES[i]),
                            any(Instant.class),
                            any(Instant.class),
                            isNull());
        }

        // Verify that items were eventually added to the queue
        assertFalse(itemInfoQueue.isEmpty(), "Queue should not be empty after successful retry");

        // Verify total number of items in queue
        assertEquals(Constants.CONTENT_TYPES.length, itemInfoQueue.size(),
                "Should have one item per content type");
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
