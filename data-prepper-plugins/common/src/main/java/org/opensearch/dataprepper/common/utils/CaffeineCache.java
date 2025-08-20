/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.utils;
import com.google.common.annotations.VisibleForTesting;

import org.opensearch.dataprepper.model.event.Event;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;

import java.lang.instrument.Instrumentation;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CaffeineCache<K, V> {

    public static final int OVERHEAD_PER_CACHE_ENTRY = 32;

    class ByteArrayValueWeigher implements Weigher<K, V> {

        @Override
        public int weigh(K key, V value) {
            return value.toString().length() + key.toString().length();
        }
    }

    private final Cache<K, V> cache;
    private final long maxBytes;
    private final int maxEntries;
    private AtomicInteger curEntries;
    private AtomicLong curBytes;

    public CaffeineCache(int maxEntries, long maxBytes, int ttlSeconds, boolean lruMode) {
        this.maxBytes = maxBytes;
        this.maxEntries = maxEntries;
        this.curEntries = new AtomicInteger(0);
        this.curBytes = new AtomicLong(0);
        Caffeine<K, V> caffeine;
        if (lruMode) {
            caffeine = (Caffeine<K, V>) Caffeine.newBuilder()
                .maximumSize(maxEntries);
        } else {
            caffeine = (Caffeine<K, V>) Caffeine.newBuilder()
                .maximumWeight(maxBytes)
                .weigher(new ByteArrayValueWeigher());
        }
        this.cache = caffeine
                .expireAfterAccess(ttlSeconds, TimeUnit.SECONDS)
                .removalListener((K k, V v, RemovalCause cause) -> {
                    int valueLength = (v instanceof String) ? ((String)v).length() : 
                                ((Event)v).toJsonString().length();
                    curBytes.addAndGet(-getEntrySize(k, v));
                    curEntries.decrementAndGet();
                })
                .recordStats()
                .build();
    }

    long getEntrySize(K key, V value) {
        long size = OVERHEAD_PER_CACHE_ENTRY;
        if (key instanceof String) {
            size += (long)((String)key).length();
        } else {
            size += 8L;
        }
        size += (value instanceof String) ? ((String)value).length() : 
                    ((Event)value).toJsonString().length();
        return size;
    }

    public void put(K key, V value) {
        if (!(value instanceof Event) && !(value instanceof String)) {
            throw new RuntimeException("Currently only Event/String type values are supported");
        }
        if (!(key instanceof String) && !(key instanceof Integer) && !(key instanceof Long)) {
            throw new RuntimeException("Currently only String/Integer/Long type keys are supported");
        }
        cache.put(key, value);
        curEntries.incrementAndGet();
        curBytes.addAndGet(getEntrySize(key, value));
        // run cleanUp to create space by removing any expired entries
        if (curEntries.get() > maxEntries/2 || curBytes.get() > maxBytes/2) {
            cache.cleanUp();
        }
    }

    public V get(K key) {
        return cache.getIfPresent(key);
    }

    public boolean containsKey(K key) {
        return cache.getIfPresent(key) != null;
    }

    public void remove(K key) {
        V value = cache.getIfPresent(key);
        if (value == null) {
            return;
        }
        cache.invalidate(key);
        cache.cleanUp();
    }

    public CacheStats getStats() {
        return cache.stats();
    }

    public void clear() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    @VisibleForTesting
    int getNumEntries() {
        return curEntries.get();
    }

    @VisibleForTesting
    long getSize() {
        return curBytes.get();
    }
}

