/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConfigurableIntervalLeaderProgressStateTest {

    private ConfigurableIntervalLeaderProgressState progressState;
    private Instant testTime;
    private int testRemainingHours;

    @BeforeEach
    void setUp() {
        testTime = Instant.now();
        testRemainingHours = 24;
        progressState = new ConfigurableIntervalLeaderProgressState(testTime, testRemainingHours);
    }

    @Test
    void testConstructorWithLastPollTimeAndRemainingHours() {
        assertEquals(testTime, progressState.getLastPollTime());
        assertEquals(testRemainingHours, progressState.getRemainingHours());
        assertNotNull(progressState.getLastPartitionTimePerDimensionType());
        assertTrue(progressState.getLastPartitionTimePerDimensionType().isEmpty());
    }

    @Test
    void testDefaultConstructor() {
        ConfigurableIntervalLeaderProgressState defaultState = new ConfigurableIntervalLeaderProgressState();
        
        assertNull(defaultState.getLastPollTime());
        assertEquals(0, defaultState.getRemainingHours());
        assertNotNull(defaultState.getLastPartitionTimePerDimensionType());
        assertTrue(defaultState.getLastPartitionTimePerDimensionType().isEmpty());
    }

    @Test
    void testGetAndSetLastPartitionTimePerDimensionType() {
        Map<String, Instant> partitionTimes = new HashMap<>();
        Instant auditLogTime = testTime.minus(Duration.ofMinutes(5));
        Instant issueTime = testTime.minus(Duration.ofMinutes(10));
        
        partitionTimes.put("auditLogEntries", auditLogTime);
        partitionTimes.put("issues", issueTime);
        
        progressState.setLastPartitionTimePerDimensionType(partitionTimes);
        
        Map<String, Instant> retrieved = progressState.getLastPartitionTimePerDimensionType();
        assertEquals(2, retrieved.size());
        assertEquals(auditLogTime, retrieved.get("auditLogEntries"));
        assertEquals(issueTime, retrieved.get("issues"));
    }

    @Test
    void testSetLastPartitionTimePerDimensionTypeWithNull() {
        progressState.setLastPartitionTimePerDimensionType(null);
        
        Map<String, Instant> retrieved = progressState.getLastPartitionTimePerDimensionType();
        assertNotNull(retrieved);
        assertTrue(retrieved.isEmpty());
    }

    @Test
    void testMapIsModifiable() {
        Map<String, Instant> partitionTimes = progressState.getLastPartitionTimePerDimensionType();
        
        // Should be able to modify the returned map
        Instant testInstant = Instant.now();
        partitionTimes.put("testType", testInstant);
        
        assertEquals(testInstant, progressState.getLastPartitionTimePerDimensionType().get("testType"));
    }

    @Test
    void testInheritedFunctionality() {
        // Test that it properly inherits from DimensionalTimeSliceLeaderProgressState
        Instant newTime = testTime.plus(Duration.ofHours(1));
        int newHours = 12;
        
        progressState.setLastPollTime(newTime);
        progressState.setRemainingHours(newHours);
        
        assertEquals(newTime, progressState.getLastPollTime());
        assertEquals(newHours, progressState.getRemainingHours());
    }

    @Test
    void testStateIndependenceFromParent() {
        // Ensure that the configurable interval specific functionality doesn't interfere
        // with the parent class functionality
        
        Map<String, Instant> partitionTimes = new HashMap<>();
        partitionTimes.put("type1", testTime);
        progressState.setLastPartitionTimePerDimensionType(partitionTimes);
        
        // Change parent state
        Instant newLastPoll = testTime.plus(Duration.ofMinutes(30));
        progressState.setLastPollTime(newLastPoll);
        
        // Both should be independent
        assertEquals(newLastPoll, progressState.getLastPollTime());
        assertEquals(testTime, progressState.getLastPartitionTimePerDimensionType().get("type1"));
    }

    @Test
    void testMultipleTimeUpdates() {
        Map<String, Instant> partitionTimes = new HashMap<>();
        
        // Initial setup
        Instant time1 = testTime.minus(Duration.ofMinutes(10));
        Instant time2 = testTime.minus(Duration.ofMinutes(5));
        partitionTimes.put("auditLogEntries", time1);
        partitionTimes.put("detections", time2);
        progressState.setLastPartitionTimePerDimensionType(partitionTimes);
        
        // Update one dimension type
        partitionTimes.put("auditLogEntries", testTime);
        
        Map<String, Instant> retrieved = progressState.getLastPartitionTimePerDimensionType();
        assertEquals(testTime, retrieved.get("auditLogEntries"));
        assertEquals(time2, retrieved.get("detections")); // Should remain unchanged
    }

    @Test
    void testEmptyMapHandling() {
        Map<String, Instant> emptyMap = new HashMap<>();
        progressState.setLastPartitionTimePerDimensionType(emptyMap);
        
        Map<String, Instant> retrieved = progressState.getLastPartitionTimePerDimensionType();
        assertNotNull(retrieved);
        assertTrue(retrieved.isEmpty());
        
        // Should be able to add to it
        retrieved.put("newType", testTime);
        assertEquals(1, progressState.getLastPartitionTimePerDimensionType().size());
    }

    @Test
    void testPartitionTimeRetrieval() {
        Map<String, Instant> partitionTimes = new HashMap<>();
        Instant auditTime = testTime.minus(Duration.ofMinutes(15));
        Instant issueTime = testTime.minus(Duration.ofMinutes(30));
        Instant detectionTime = testTime.minus(Duration.ofMinutes(2));
        
        partitionTimes.put("auditLogEntries", auditTime);
        partitionTimes.put("issues", issueTime);
        partitionTimes.put("detections", detectionTime);
        
        progressState.setLastPartitionTimePerDimensionType(partitionTimes);
        
        // Verify individual retrievals
        Map<String, Instant> all = progressState.getLastPartitionTimePerDimensionType();
        assertEquals(auditTime, all.get("auditLogEntries"));
        assertEquals(issueTime, all.get("issues"));  
        assertEquals(detectionTime, all.get("detections"));
        assertNull(all.get("nonExistentType"));
    }

    @Test
    void testStateConsistency() {
        // Test that the state remains consistent when both parent and child properties are modified
        Instant parentTime = testTime.plus(Duration.ofHours(2));
        int parentHours = 48;
        
        Map<String, Instant> childMap = new HashMap<>();
        childMap.put("type1", testTime.minus(Duration.ofMinutes(5)));
        childMap.put("type2", testTime.minus(Duration.ofMinutes(10)));
        
        // Set both parent and child properties
        progressState.setLastPollTime(parentTime);
        progressState.setRemainingHours(parentHours);
        progressState.setLastPartitionTimePerDimensionType(childMap);
        
        // Verify both are maintained correctly
        assertEquals(parentTime, progressState.getLastPollTime());
        assertEquals(parentHours, progressState.getRemainingHours());
        assertEquals(2, progressState.getLastPartitionTimePerDimensionType().size());
        assertEquals(testTime.minus(Duration.ofMinutes(5)), 
                     progressState.getLastPartitionTimePerDimensionType().get("type1"));
    }
}
