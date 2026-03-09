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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.S3EnrichProcessorConfig;

import java.time.Duration;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CacheFactoryTest {

    @Mock
    private S3EnrichProcessorConfig config;

    @BeforeEach
    void setUp() {
        when(config.getCacheTtl()).thenReturn(Duration.ofMinutes(10));
        when(config.getCacheSizeLimit()).thenReturn(1000);
    }

    private CacheFactory createObjectUnderTest() {
        return new CacheFactory(config);
    }

    @Test
    void constructor_builds_s3Cache_on_initialization() {
        final CacheFactory objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.getS3Cache(), notNullValue());
    }

    @Test
    void getS3Cache_returns_same_instance_across_calls() {
        final CacheFactory objectUnderTest = createObjectUnderTest();

        final Cache<String, Cache<String, Event>> first = objectUnderTest.getS3Cache();
        final Cache<String, Cache<String, Event>> second = objectUnderTest.getS3Cache();

        assertThat(first, notNullValue());
        assertThat(first == second, org.hamcrest.CoreMatchers.equalTo(true));
    }

    @Test
    void createEventsCache_returns_new_cache_per_call() {
        final CacheFactory objectUnderTest = createObjectUnderTest();

        final Cache<String, Event> first = objectUnderTest.createEventsCache("s3://bucket/key1");
        final Cache<String, Event> second = objectUnderTest.createEventsCache("s3://bucket/key2");

        assertThat(first, notNullValue());
        assertThat(second, notNullValue());
        assertThat(first != second, org.hamcrest.CoreMatchers.equalTo(true));
    }

    @Test
    void createEventsCache_accepts_configured_max_size() {
        when(config.getCacheSizeLimit()).thenReturn(500);
        final CacheFactory objectUnderTest = createObjectUnderTest();

        final Cache<String, Event> cache = objectUnderTest.createEventsCache("s3://bucket/key");

        assertThat(cache, notNullValue());
    }

    @Test
    void createEventsCache_with_short_ttl_still_creates_valid_cache() {
        when(config.getCacheTtl()).thenReturn(Duration.ofSeconds(1));
        final CacheFactory objectUnderTest = createObjectUnderTest();

        final Cache<String, Event> cache = objectUnderTest.createEventsCache("s3://bucket/key");

        assertThat(cache, notNullValue());
    }
}
