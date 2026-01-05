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
import lombok.Getter;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.s3_enrich.processor.S3EnrichProcessorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class CacheFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CacheFactory.class);

    private final S3EnrichProcessorConfig config;

    /** SINGLETON cache: S3 URL -> (recordId -> Event cache)
     * -- GETTER --
     * Return singleton outer cache
     */
    @Getter
    private final Cache<String, Cache<String, Event>> s3Cache;

    public CacheFactory(S3EnrichProcessorConfig config) {
        this.config = config;
        this.s3Cache = buildS3Cache(); // created ONCE
    }

    /** Build outer cache once */
    private Cache<String, Cache<String, Event>> buildS3Cache() {
        Duration cacheTtl = config.getCacheTtl();
        int outerMaxSize = 100;

        LOG.info("Initializing SINGLETON S3 URL Cache: maxSize={} ttl={}m",
                outerMaxSize, cacheTtl.toMinutes());

        return Caffeine.newBuilder()
                .maximumSize(outerMaxSize)
                .expireAfterAccess(cacheTtl)
                .recordStats()
                .removalListener((key, value, cause) ->
                        LOG.trace("[Outer S3 URL Eviction] key={} cause={}", key, cause))
                .build();
    }

    /** Inner cache builder (1 per S3 URL) */
    public Cache<String, Event> createEventsCache(String s3Url) {
        int maxSize = config.getCacheSizeLimit();
        Duration cacheTtl = config.getCacheTtl();

        LOG.info("Creating Inner Events Cache for {}: maxSize={} ttl={}m",
                s3Url, maxSize, cacheTtl.toMinutes());

        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(cacheTtl)
                .recordStats()
                .removalListener((recordId, event, cause) ->
                        LOG.trace("[Inner Event Eviction] s3Url={} recordId={} cause={}",
                                s3Url, recordId, cause))
                .build();
    }

}
