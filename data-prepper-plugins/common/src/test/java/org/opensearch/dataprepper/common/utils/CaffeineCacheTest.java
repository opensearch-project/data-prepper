/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.Map;

public class CaffeineCacheTest {
    static private final String TEST_EVENT_TYPE="event";
    private CaffeineCache<String, Object> cache;

    CaffeineCache createObjectUnderTest(int maxEntries, long maxBytes, int ttlSeconds, boolean strictLRU) {
        return new CaffeineCache<String, Object>(maxEntries, maxBytes, ttlSeconds, strictLRU);
    }

    @Test
    public void test_CaffeineCacheTestWithEvents() {
        cache = createObjectUnderTest(100, 100, 10, true);
        String key1 = "key1";
        Map<String, Object> data = Map.of("k1", "value1", "k2", "value2");
        Event testEvent1 = JacksonEvent.builder()
                          .withData(data)
                          .withEventType(TEST_EVENT_TYPE)
                          .build();
        String key2 = "key2";
        data = Map.of("k11", "value11", "k22", "value22");
        Event testEvent2 = JacksonEvent.builder()
                          .withData(data)
                          .withEventType(TEST_EVENT_TYPE)
                          .build();
        cache.put(key1, testEvent1);
        assertThat(cache.get(key1), equalTo(testEvent1));
        assertThat(cache.getNumEntries(), equalTo(1));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, testEvent1)));
        CacheStats stats = cache.getStats();
        assertThat(stats.hitCount(), equalTo(1L));
        cache.put(key2, testEvent2);
        assertThat(cache.get(key2), equalTo(testEvent2));
        assertThat(cache.getNumEntries(), equalTo(2));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, testEvent1) + cache.getEntrySize(key2, testEvent2)));
        stats = cache.getStats();
        assertThat(stats.hitCount(), equalTo(2L));
    }

    @Test
    public void test_CaffeineCacheBasic_LRUMode() {
        cache = createObjectUnderTest(100, 100, 10, true);
        String value1 = "value1";
        String key1 = "key1";
        cache.put(key1, value1);
        assertThat(cache.get(key1), equalTo(value1));
        assertThat(cache.getNumEntries(), equalTo(1));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1)));
        String key2 = "key2";
        String value2 = "value2";
        cache.put(key2, value2);
        assertThat(cache.get(key2), equalTo(value2));
        assertThat(cache.getNumEntries(), equalTo(2));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1) + cache.getEntrySize(key2, value2)));
        cache.clear();
        assertFalse(cache.containsKey(key1));
        assertFalse(cache.containsKey(key2));
        assertThat(cache.getNumEntries(), equalTo(0));
        assertThat(cache.getSize(), equalTo(0L));
    }

    @Test
    public void test_CaffeineCacheBasic_TinyLFUMode() {
        cache = createObjectUnderTest(100, 100, 10, false);
        String value1 = "value1";
        String key1 = "key1";
        cache.put(key1, value1);
        assertThat(cache.get(key1), equalTo(value1));
        assertThat(cache.getNumEntries(), equalTo(1));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1)));
        String key2 = "key2";
        String value2 = "value2";
        cache.put(key2, value2);
        assertThat(cache.get(key2), equalTo(value2));
        assertThat(cache.getNumEntries(), equalTo(2));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1) + cache.getEntrySize(key2, value2)));
        cache.clear();
        assertFalse(cache.containsKey(key1));
        assertFalse(cache.containsKey(key2));
        assertThat(cache.getNumEntries(), equalTo(0));
        assertThat(cache.getSize(), equalTo(0L));
    }

    @Test
    public void test_CaffeineCache_eviction_when_capacity_exceeds() {
        cache = createObjectUnderTest(2, 100, 10, true);
        String key1 = "key1";
        String value1 = "value1";
        cache.put(key1, value1);
        assertThat(cache.getNumEntries(), equalTo(1));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1)));
        String key2 = "key2";
        String value2 = "value2";
        cache.put(key2, value2);
        assertThat(cache.getNumEntries(), equalTo(2));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1) + cache.getEntrySize(key2, value2)));
        assertThat(cache.get(key1), equalTo(value1));
        assertThat(cache.get(key2), equalTo(value2));
        assertTrue(cache.containsKey(key2));
        String key3 = "key3";
        String value3 = "value33";
        cache.put(key3, value3);
        assertTrue(cache.containsKey(key3));
        assertTrue(cache.containsKey(key2));
        assertFalse(cache.containsKey(key1));
        assertThat(cache.getNumEntries(), equalTo(2));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key3, value3) + cache.getEntrySize(key2, value2)));
    }

    @Test
    public void test_CaffeineCache_eviction_when_capacity_exceeds_TinyLFUMode() {
        cache = createObjectUnderTest(2, 20, 10, false);
        String value1 = "value1";
        String key1 = "key1";
        cache.put(key1, value1);
        assertThat(cache.getNumEntries(), equalTo(1));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1)));
        String value2 = "value2";
        String key2 = "key2";
        cache.put(key2, value2);
        assertThat(cache.getNumEntries(), equalTo(2));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1) + cache.getEntrySize(key2, value2)));
        assertThat(cache.get(key1), equalTo(value1));
        assertThat(cache.get(key2), equalTo(value2));
        String key3 = "key3";
        String value3 = "value33";
        cache.put(key3, value3);
        assertTrue(cache.containsKey(key2));
        assertTrue(cache.containsKey(key1));
        assertFalse(cache.containsKey(key3));
        assertThat(cache.getNumEntries(), equalTo(2));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1) + cache.getEntrySize(key2, value2)));
    }

    @Test
    public void test_CaffeineCache_put_after_expiry() {
        cache = createObjectUnderTest(10, 100, 2, true);
        String value1 = "value1";
        String key1 = "key1";
        cache.put(key1, value1);
        assertThat(cache.getNumEntries(), equalTo(1));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1)));
        String value2 = "value2";
        String key2 = "key2";
        cache.put(key2, value2);
        assertThat(cache.getNumEntries(), equalTo(2));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1) + cache.getEntrySize(key2, value2)));
        assertThat(cache.get(key1), equalTo(value1));
        assertThat(cache.get(key2), equalTo(value2));
        try {
            Thread.sleep(3);
        } catch (Exception e){}
        cache.clear();
        String key3="key3";
        String value3 = "value33";
        cache.put(key3, value3);
        assertTrue(cache.containsKey(key3));
        assertFalse(cache.containsKey(key2));
        assertFalse(cache.containsKey(key1));
        assertThat(cache.getNumEntries(), equalTo(1));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key3, value3)));
    }
 
    @Test
    public void test_CaffeineCache_put_after_expiry_TinyLFUMode() {
        cache = createObjectUnderTest(10, 100, 2, false);
        String value1 = "value1";
        String key1="key1";
        cache.put("key1", value1);
        assertThat(cache.getNumEntries(), equalTo(1));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1)));
        String key2="key2";
        String value2 = "value2";
        cache.put("key2", value2);
        assertThat(cache.getNumEntries(), equalTo(2));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1) + cache.getEntrySize(key2, value2)));
        assertThat(cache.get(key1), equalTo(value1));
        assertThat(cache.get(key2), equalTo(value2));
        try {
            Thread.sleep(3);
        } catch (Exception e){}
        cache.clear();
        String value3 = "value33";
        String key3="key3";
        cache.put(key3, value3);
        assertTrue(cache.containsKey(key3));
        assertFalse(cache.containsKey(key2));
        assertFalse(cache.containsKey(key1));
        assertThat(cache.getNumEntries(), equalTo(1));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key3, value3)));
    }

    @Test
    public void testCacheRemove() {
        cache = createObjectUnderTest(10, 100, 2, false);
        String value1 = "value1";
        String key1="key1";
        cache.put("key1", value1);
        assertThat(cache.getNumEntries(), equalTo(1));
        assertThat(cache.getSize(), equalTo(cache.getEntrySize(key1, value1)));
        assertTrue(cache.containsKey(key1));
        cache.remove(key1);
        assertFalse(cache.containsKey(key1));
    }

}
