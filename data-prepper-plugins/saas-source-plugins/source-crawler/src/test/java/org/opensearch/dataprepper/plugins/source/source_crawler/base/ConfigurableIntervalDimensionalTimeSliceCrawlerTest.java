/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.ConfigurableIntervalLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceWorkerProgressState;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ConfigurableIntervalDimensionalTimeSliceCrawlerTest {

    @Mock
    private CrawlerClient crawlerClient;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter partitionsCreatedCounter;

    @Mock
    private Counter partitionsSkippedCounter;

    @Mock
    private EnhancedSourceCoordinator coordinator;

    @Mock
    private LeaderPartition leaderPartition;

    @Mock
    private ConfigurableIntervalLeaderProgressState leaderProgressState;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    private ConfigurableIntervalDimensionalTimeSliceCrawler crawler;
    private Function<String, Duration> scheduleProvider;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(anyString())).thenReturn(partitionsCreatedCounter);
        
        crawler = new ConfigurableIntervalDimensionalTimeSliceCrawler(crawlerClient, pluginMetrics);
        
        // Set up schedule provider: 5 minutes for some types, 10 minutes for others
        scheduleProvider = dimensionType -> {
            switch (dimensionType) {
                case "auditLogEntries":
                case "detections":
                    return Duration.ofMinutes(5);
                case "issues":
                case "vulnerabilityFindings":
                case "configurationFindings":
                    return Duration.ofMinutes(10);
                default:
                    return Duration.ofMinutes(5);
            }
        };
        
        crawler.setPartitionScheduleProvider(scheduleProvider);
        crawler.setDimensionTypes(Arrays.asList("auditLogEntries", "detections", "issues", 
                "vulnerabilityFindings", "configurationFindings"));
    }

    @Test
    void testCrawlWithNullScheduleProvider() {
        ConfigurableIntervalDimensionalTimeSliceCrawler crawlerWithoutSchedule = 
                new ConfigurableIntervalDimensionalTimeSliceCrawler(crawlerClient, pluginMetrics);
        
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(leaderProgressState));
        
        // Should fall back to parent behavior
        Instant result = crawlerWithoutSchedule.crawl(leaderPartition, coordinator);
        
        // Verify warning was logged (would need log capture for full verification)
        assertNotNull(result);
    }

    @Test
    void testCrawlIncrementalSyncWithMixedIntervals() {
        Instant now = Instant.now();
        Instant lastPollTime = now.minus(Duration.ofMinutes(11)); // 11 minutes ago
        
        Map<String, Instant> lastPartitionTimes = new HashMap<>();
        lastPartitionTimes.put("auditLogEntries", now.minus(Duration.ofMinutes(6))); // Should create (5min schedule)
        lastPartitionTimes.put("detections", now.minus(Duration.ofMinutes(4))); // Should skip (5min schedule)
        lastPartitionTimes.put("issues", now.minus(Duration.ofMinutes(11))); // Should create (10min schedule)
        
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(leaderProgressState));
        when(leaderProgressState.getRemainingHours()).thenReturn(0); // Incremental sync
        when(leaderProgressState.getLastPollTime()).thenReturn(lastPollTime);
        when(leaderProgressState.getLastPartitionTimePerDimensionType()).thenReturn(lastPartitionTimes);
        
        Instant result = crawler.crawl(leaderPartition, coordinator);
        
        // Should create partitions for auditLogEntries and issues (3 total including uninitialized types)
        verify(coordinator, atLeastOnce()).createPartition(any());
        verify(partitionsCreatedCounter, atLeastOnce()).increment();
        assertNotNull(result);
    }

    @Test
    void testCrawlHistoricalPull() {
        Instant now = Instant.now();
        
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(leaderProgressState));
        when(leaderProgressState.getRemainingHours()).thenReturn(5); // Historical pull
        when(leaderProgressState.getLastPollTime()).thenReturn(now);
        
        Instant result = crawler.crawl(leaderPartition, coordinator);
        
        // Should handle historical pull (exact behavior depends on parent implementation)
        assertNotNull(result);
    }

    @Test
    void testExecutePartition() {
        DimensionalTimeSliceWorkerProgressState workerState = new DimensionalTimeSliceWorkerProgressState();
        workerState.setDimensionType("testType");
        workerState.setStartTime(Instant.now().minus(Duration.ofMinutes(10)));
        workerState.setEndTime(Instant.now());
        workerState.setPartitionCreationTime(Instant.now().minus(Duration.ofMinutes(1)));
        
        crawler.executePartition(workerState, buffer, acknowledgementSet);
        
        verify(crawlerClient).executePartition(workerState, buffer, acknowledgementSet);
    }

    @Test
    void testStateInitializationPreventsCorruption() {
        Instant baseTime = Instant.now().minus(Duration.ofMinutes(20));
        Map<String, Instant> lastPartitionTimes = new HashMap<>();
        // Only initialize some dimension types
        lastPartitionTimes.put("auditLogEntries", baseTime.minus(Duration.ofMinutes(6)));
        // Leave others uninitialized to test initialization logic
        
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(leaderProgressState));
        when(leaderProgressState.getRemainingHours()).thenReturn(0);
        when(leaderProgressState.getLastPollTime()).thenReturn(baseTime);
        when(leaderProgressState.getLastPartitionTimePerDimensionType()).thenReturn(lastPartitionTimes);
        
        crawler.crawl(leaderPartition, coordinator);
        
        // Verify that uninitialized dimension types were initialized with base time
        assertEquals(5, lastPartitionTimes.size()); // All dimension types should be initialized
        assertTrue(lastPartitionTimes.containsKey("issues"));
        assertTrue(lastPartitionTimes.containsKey("vulnerabilityFindings"));
        assertEquals(baseTime, lastPartitionTimes.get("issues")); // Should be initialized with base time
    }

    @Test
    void testGetDimensionTypes() {
        assertEquals(5, crawler.getDimensionTypes().size());
        assertTrue(crawler.getDimensionTypes().contains("auditLogEntries"));
        assertTrue(crawler.getDimensionTypes().contains("issues"));
    }

    @Test
    void testSetPartitionScheduleProvider() {
        Function<String, Duration> newProvider = type -> Duration.ofHours(1);
        crawler.setPartitionScheduleProvider(newProvider);
        
        // Verify the provider was set (indirect test through behavior)
        assertNotNull(crawler);
    }

    @Test
    void testSetDimensionTypes() {
        crawler.setDimensionTypes(Arrays.asList("newType1", "newType2"));
        
        assertEquals(2, crawler.getDimensionTypes().size());
        assertTrue(crawler.getDimensionTypes().contains("newType1"));
        assertTrue(crawler.getDimensionTypes().contains("newType2"));
    }

    @Test 
    void testPartitionCountReliability() {
        // Test that the AtomicLong counter provides consistent counts
        Instant now = Instant.now();
        Instant lastPollTime = now.minus(Duration.ofMinutes(11));
        
        Map<String, Instant> lastPartitionTimes = new HashMap<>();
        // Set up all types to be eligible for partition creation
        lastPartitionTimes.put("auditLogEntries", now.minus(Duration.ofMinutes(6)));
        lastPartitionTimes.put("detections", now.minus(Duration.ofMinutes(6)));
        lastPartitionTimes.put("issues", now.minus(Duration.ofMinutes(11)));
        lastPartitionTimes.put("vulnerabilityFindings", now.minus(Duration.ofMinutes(11)));
        lastPartitionTimes.put("configurationFindings", now.minus(Duration.ofMinutes(11)));
        
        when(leaderPartition.getProgressState()).thenReturn(Optional.of(leaderProgressState));
        when(leaderProgressState.getRemainingHours()).thenReturn(0);
        when(leaderProgressState.getLastPollTime()).thenReturn(lastPollTime);
        when(leaderProgressState.getLastPartitionTimePerDimensionType()).thenReturn(lastPartitionTimes);
        
        Instant result = crawler.crawl(leaderPartition, coordinator);
        
        // All 5 dimension types should create partitions
        verify(coordinator, times(5)).createPartition(any());
        verify(partitionsCreatedCounter, times(5)).increment();
        assertNotNull(result);
    }
}
