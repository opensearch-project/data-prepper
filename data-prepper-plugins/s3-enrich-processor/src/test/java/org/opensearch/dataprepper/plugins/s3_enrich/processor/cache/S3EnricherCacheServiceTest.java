/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.s3_enrich.processor.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3EnricherCacheServiceTest {

    @Mock
    private CacheFactory cacheFactory;

    private S3EnricherCacheService objectUnderTest;

    @BeforeEach
    void setUp() {
        final Cache<String, Cache<String, Event>> outerCache = Caffeine.newBuilder()
                .maximumSize(100)
                .expireAfterAccess(Duration.ofMinutes(10))
                .build();
        when(cacheFactory.getS3Cache()).thenReturn(outerCache);
        objectUnderTest = new S3EnricherCacheService(cacheFactory);
    }

    @Test
    void getOrCreateRecordCache_creates_new_cache_for_unknown_s3Url() {
        final String s3Url = "s3://bucket/key";
        final Cache<String, Event> newCache = Caffeine.newBuilder().maximumSize(100).build();
        when(cacheFactory.createEventsCache(s3Url)).thenReturn(newCache);

        final Cache<String, Event> result = objectUnderTest.getOrCreateRecordCache(s3Url);

        assertThat(result, notNullValue());
        verify(cacheFactory).createEventsCache(s3Url);
    }

    @Test
    void getOrCreateRecordCache_returns_existing_cache_for_known_s3Url() {
        final String s3Url = "s3://bucket/key";
        final Cache<String, Event> newCache = Caffeine.newBuilder().maximumSize(100).build();
        when(cacheFactory.createEventsCache(s3Url)).thenReturn(newCache);

        objectUnderTest.getOrCreateRecordCache(s3Url);
        final Cache<String, Event> result = objectUnderTest.getOrCreateRecordCache(s3Url);

        assertThat(result, notNullValue());
        verify(cacheFactory, times(1)).createEventsCache(s3Url); // only created once
    }

    @Test
    void getRecordCacheIfPresent_returns_null_for_unknown_s3Url() {
        final Cache<String, Event> result = objectUnderTest.getRecordCacheIfPresent("s3://bucket/unknown");

        assertThat(result, nullValue());
    }

    @Test
    void getRecordCacheIfPresent_returns_cache_for_known_s3Url() {
        final String s3Url = "s3://bucket/key";
        final Cache<String, Event> newCache = Caffeine.newBuilder().maximumSize(100).build();
        when(cacheFactory.createEventsCache(s3Url)).thenReturn(newCache);
        objectUnderTest.getOrCreateRecordCache(s3Url);

        final Cache<String, Event> result = objectUnderTest.getRecordCacheIfPresent(s3Url);

        assertThat(result, notNullValue());
    }

    @Test
    void put_and_get_returns_stored_event() {
        final String s3Url = "s3://bucket/key";
        final String recordId = UUID.randomUUID().toString();
        final Event event = mock(Event.class);
        final Cache<String, Event> innerCache = Caffeine.newBuilder().maximumSize(100).build();
        when(cacheFactory.createEventsCache(s3Url)).thenReturn(innerCache);

        objectUnderTest.put(s3Url, recordId, event);
        final Event result = objectUnderTest.get(s3Url, recordId);

        assertThat(result, equalTo(event));
    }

    @Test
    void get_returns_null_for_unknown_s3Url() {
        final Event result = objectUnderTest.get("s3://bucket/unknown", "record-id");

        assertThat(result, nullValue());
    }

    @Test
    void get_returns_null_for_unknown_recordId() {
        final String s3Url = "s3://bucket/key";
        final Cache<String, Event> innerCache = Caffeine.newBuilder().maximumSize(100).build();
        when(cacheFactory.createEventsCache(s3Url)).thenReturn(innerCache);
        objectUnderTest.put(s3Url, "record-id-1", mock(Event.class));

        final Event result = objectUnderTest.get(s3Url, "record-id-unknown");

        assertThat(result, nullValue());
    }

    @Test
    void containsS3Uri_returns_false_for_unknown_uri() {
        assertThat(objectUnderTest.containsS3Uri("s3://bucket/unknown"), equalTo(false));
    }

    @Test
    void containsS3Uri_returns_true_after_put() {
        final String s3Url = "s3://bucket/key";
        final Cache<String, Event> innerCache = Caffeine.newBuilder().maximumSize(100).build();
        when(cacheFactory.createEventsCache(s3Url)).thenReturn(innerCache);
        objectUnderTest.put(s3Url, "record-id", mock(Event.class));

        assertThat(objectUnderTest.containsS3Uri(s3Url), equalTo(true));
    }

    @Test
    void loadIfAbsent_calls_loader_when_not_cached() {
        final String s3Url = "s3://bucket/key";
        final AtomicInteger callCount = new AtomicInteger(0);

        objectUnderTest.loadIfAbsent(s3Url, callCount::incrementAndGet);

        assertThat(callCount.get(), equalTo(1));
    }

    @Test
    void loadIfAbsent_does_not_call_loader_when_already_cached() {
        final String s3Url = "s3://bucket/key";
        final Cache<String, Event> innerCache = Caffeine.newBuilder().maximumSize(100).build();
        when(cacheFactory.createEventsCache(s3Url)).thenReturn(innerCache);
        objectUnderTest.put(s3Url, "record-id", mock(Event.class));
        final AtomicInteger callCount = new AtomicInteger(0);

        objectUnderTest.loadIfAbsent(s3Url, callCount::incrementAndGet);

        assertThat(callCount.get(), equalTo(0));
    }

    @Test
    void loadIfAbsent_loader_is_called_exactly_once_even_if_called_multiple_times_for_same_uri() {
        final String s3Url = "s3://bucket/key";
        final Cache<String, Event> innerCache = Caffeine.newBuilder().maximumSize(100).build();
        final AtomicInteger callCount = new AtomicInteger(0);
        when(cacheFactory.createEventsCache(anyString())).thenReturn(innerCache);

        // First call - not cached, so loader runs
        objectUnderTest.loadIfAbsent(s3Url, () -> {
            callCount.incrementAndGet();
            objectUnderTest.put(s3Url, "record-id", mock(Event.class)); // populate cache in loader
        });

        // Second call - now cached, loader should NOT run
        objectUnderTest.loadIfAbsent(s3Url, callCount::incrementAndGet);

        assertThat(callCount.get(), equalTo(1));
    }

    @Test
    void getOrLoadRecordCache_calls_loader_when_not_cached() {
        final String s3Url = "s3://bucket/key";
        final Cache<String, Event> innerCache = Caffeine.newBuilder().maximumSize(100).build();

        final Cache<String, Event> result = objectUnderTest.getOrLoadRecordCache(s3Url, () -> innerCache);

        assertThat(result, equalTo(innerCache));
    }

    @Test
    void getOrLoadRecordCache_returns_existing_cache_when_already_cached() {
        final String s3Url = "s3://bucket/key";
        final Cache<String, Event> firstCache = Caffeine.newBuilder().maximumSize(100).build();
        final Cache<String, Event> secondCache = Caffeine.newBuilder().maximumSize(100).build();

        objectUnderTest.getOrLoadRecordCache(s3Url, () -> firstCache);
        final Cache<String, Event> result = objectUnderTest.getOrLoadRecordCache(s3Url, () -> secondCache);

        assertThat(result, equalTo(firstCache));
    }

    @Test
    void getRecordCount_returns_zero_for_unknown_s3Url() {
        assertThat(objectUnderTest.getRecordCount("s3://bucket/unknown"), equalTo(0L));
    }

    @Test
    void getRecordCount_returns_count_of_stored_records() {
        final String s3Url = "s3://bucket/key";
        final Cache<String, Event> innerCache = Caffeine.newBuilder().maximumSize(100).build();
        when(cacheFactory.createEventsCache(s3Url)).thenReturn(innerCache);
        objectUnderTest.put(s3Url, "record-1", mock(Event.class));
        objectUnderTest.put(s3Url, "record-2", mock(Event.class));

        assertThat(objectUnderTest.getRecordCount(s3Url), equalTo(2L));
    }

    @Test
    void clearAll_removes_all_cached_entries() {
        final String s3Url1 = "s3://bucket/key1";
        final String s3Url2 = "s3://bucket/key2";
        final Cache<String, Event> innerCache1 = Caffeine.newBuilder().maximumSize(100).build();
        final Cache<String, Event> innerCache2 = Caffeine.newBuilder().maximumSize(100).build();
        when(cacheFactory.createEventsCache(s3Url1)).thenReturn(innerCache1);
        when(cacheFactory.createEventsCache(s3Url2)).thenReturn(innerCache2);

        objectUnderTest.put(s3Url1, "record-1", mock(Event.class));
        objectUnderTest.put(s3Url2, "record-2", mock(Event.class));

        objectUnderTest.clearAll();

        assertThat(objectUnderTest.containsS3Uri(s3Url1), equalTo(false));
        assertThat(objectUnderTest.containsS3Uri(s3Url2), equalTo(false));
    }

    @Test
    void clearS3Uri_removes_only_specified_s3Url() {
        final String s3Url1 = "s3://bucket/key1";
        final String s3Url2 = "s3://bucket/key2";
        final Cache<String, Event> innerCache1 = Caffeine.newBuilder().maximumSize(100).build();
        final Cache<String, Event> innerCache2 = Caffeine.newBuilder().maximumSize(100).build();
        when(cacheFactory.createEventsCache(s3Url1)).thenReturn(innerCache1);
        when(cacheFactory.createEventsCache(s3Url2)).thenReturn(innerCache2);

        objectUnderTest.put(s3Url1, "record-1", mock(Event.class));
        objectUnderTest.put(s3Url2, "record-2", mock(Event.class));

        objectUnderTest.clearS3Uri(s3Url1);

        assertThat(objectUnderTest.containsS3Uri(s3Url1), equalTo(false));
        assertThat(objectUnderTest.containsS3Uri(s3Url2), equalTo(true));
    }

    @Test
    void clearS3Uri_does_not_throw_for_unknown_uri() {
        objectUnderTest.clearS3Uri("s3://bucket/nonexistent");
        // no exception expected
    }
}
