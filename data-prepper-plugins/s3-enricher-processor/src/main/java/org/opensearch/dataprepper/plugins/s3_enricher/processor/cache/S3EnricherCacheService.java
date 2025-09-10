/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.s3_enricher.processor.cache;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.Getter;
import org.opensearch.dataprepper.model.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

public class S3EnricherCacheService {
    private static final Logger LOG = LoggerFactory.getLogger(S3EnricherCacheService.class);

    @Getter
    private final Cache<String, Cache<String, Event>> s3Cache;
    private final CacheFactory cacheFactory;

    // Locks for coordinating S3 object loading
    private final ConcurrentHashMap<String, ReentrantLock> loadingLocks = new ConcurrentHashMap<>();

    public S3EnricherCacheService(CacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
        this.s3Cache = cacheFactory.getS3Cache();
    }

    /**
     * Get or create per-S3URL record cache.
     * Use this when you INTEND to populate the cache.
     */
    public Cache<String, Event> getOrCreateRecordCache(String s3Url) {
        return s3Cache.get(s3Url, key -> cacheFactory.createEventsCache(s3Url));
    }

    /**
     * Get existing record cache WITHOUT creating one.
     * Returns null if s3Url not in cache.
     */
    public Cache<String, Event> getRecordCacheIfPresent(String s3Url) {
        return s3Cache.getIfPresent(s3Url);
    }

    /**
     * Put event - creates inner cache if needed.
     */
    public void put(String s3Url, String recordId, Event event) {
        getOrCreateRecordCache(s3Url).put(recordId, event);
    }

    /**
     * Get event - returns null if s3Url or recordId not found.
     * Does NOT create empty cache as side effect.
     */
    public Event get(String s3Url, String recordId) {
        Cache<String, Event> recordCache = s3Cache.getIfPresent(s3Url);
        if (recordCache == null) {
            return null;
        }
        return recordCache.getIfPresent(recordId);
    }

    /**
     * Check if s3Url exists in cache (without side effects).
     */
    public boolean containsS3Uri(String s3Url) {
        return s3Cache.getIfPresent(s3Url) != null;
    }

    /**
     * Thread-safe method to load S3 data if not already cached.
     * Ensures only ONE thread loads a given S3 URI.
     *
     * @param s3Url the S3 URI to check/load
     * @param loader the function to load data (called only if not cached)
     */
    public void loadIfAbsent(String s3Url, Runnable loader) {
        // Fast path: already cached
        if (containsS3Uri(s3Url)) {
            return;
        }

        ReentrantLock lock = loadingLocks.computeIfAbsent(s3Url, k -> new ReentrantLock());

        lock.lock();
        try {
            // Double-check after acquiring lock
            if (containsS3Uri(s3Url)) {
                return;
            }
            loader.run();
        } finally {
            lock.unlock();
            loadingLocks.remove(s3Url, lock);
        }
    }

    /**
     * Alternative: Atomic compute pattern using Caffeine's built-in mechanism.
     * Returns the record cache, loading from S3 if necessary.
     */
    public Cache<String, Event> getOrLoadRecordCache(String s3Url, Supplier<Cache<String, Event>> loader) {
        return s3Cache.get(s3Url, key -> {
            LOG.info("Loading S3 data for: {}", key);
            return loader.get();
        });
    }

    /**
     * Get count of records for a given s3Url.
     */
    public long getRecordCount(String s3Url) {
        Cache<String, Event> recordCache = s3Cache.getIfPresent(s3Url);
        return recordCache != null ? recordCache.estimatedSize() : 0;
    }

    /**
     * Clear entire cache hierarchy.
     */
    public void clearAll() {
        s3Cache.asMap().values().forEach(Cache::invalidateAll);
        s3Cache.invalidateAll();
        loadingLocks.clear();
    }

    /**
     * Clear cache for specific s3Url.
     */
    public void clearS3Uri(String s3Url) {
        Cache<String, Event> recordCache = s3Cache.getIfPresent(s3Url);
        if (recordCache != null) {
            recordCache.invalidateAll();
            s3Cache.invalidate(s3Url);
        }
        loadingLocks.remove(s3Url);
    }
}
