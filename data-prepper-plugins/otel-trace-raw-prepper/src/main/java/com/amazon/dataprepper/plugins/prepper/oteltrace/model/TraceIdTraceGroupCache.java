package com.amazon.dataprepper.plugins.prepper.oteltrace.model;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

public class TraceIdTraceGroupCache {

    private final Cache<String, String> map;

    public TraceIdTraceGroupCache(final int concurrencyLevel, final long maximumSize, final long ttlSeconds) {
        map = CacheBuilder.newBuilder()
                .concurrencyLevel(concurrencyLevel)
                .maximumSize(maximumSize)
                .expireAfterWrite(ttlSeconds, TimeUnit.SECONDS)
                .build();
    }

    public void put(final String key, final String value) {
        map.put(key, value);
    }

    public String get(final String key) {
        return map.getIfPresent(key);
    }

    public long size() {
        return map.size();
    }

    public void delete() {
        map.cleanUp();
    }
}
