package com.amazon.dataprepper.plugins.prepper.oteltrace.model;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TraceIdTraceGroupCacheTests {
    private static final long TEST_TTL_SEC = 3;
    private static final long TEST_MAX_SIZE = 2;

    TraceIdTraceGroupCache map;

    @Before
    public void setUp() {
        map = new TraceIdTraceGroupCache(1, TEST_MAX_SIZE, TEST_TTL_SEC);
    }

    @After
    public void tearDown() {
        map.delete();
    }

    @Test
    public void testAPIs() throws InterruptedException {
        // Given
        String testTraceId = "testTraceId";
        String testTraceGroup = "testTraceGroup";

        // When
        map.put(testTraceId, testTraceGroup);

        // Then
        assertEquals(1, map.size());

        // When
        String retrievedTraceGroup = map.get(testTraceId);

        // Then
        assertEquals(testTraceGroup, retrievedTraceGroup);
    }

    @Test
    public void testEviction() throws InterruptedException {
        // Given
        for (int i = 0; i < 3; i++) {
            map.put(String.format("testTraceId_%d", i), String.format("testTraceGroup_%d", i));
        }

        // When
        String retrievedTraceGroup0 = map.get("testTraceId_0");
        String retrievedTraceGroup1 = map.get("testTraceId_1");
        String retrievedTraceGroup2 = map.get("testTraceId_2");

        // Then
        assertNull(retrievedTraceGroup0);
        assertEquals("testTraceGroup_1", retrievedTraceGroup1);
        assertEquals("testTraceGroup_2", retrievedTraceGroup2);

        // When
        Thread.sleep(TimeUnit.SECONDS.toMillis(TEST_TTL_SEC));
        retrievedTraceGroup1 = map.get("testTraceId_1");
        retrievedTraceGroup2 = map.get("testTraceId_2");

        // Then
        assertNull(retrievedTraceGroup1);
        assertNull(retrievedTraceGroup2);
    }
}
