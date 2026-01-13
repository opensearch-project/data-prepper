package org.opensearch.dataprepper.plugins.s3_enricher.processor.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.s3_enricher.processor.S3EnricherProcessorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.TimeUnit;

public class CacheFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CacheFactory.class);

    private final S3EnricherProcessorConfig config;

    /** SINGLETON cache: S3 URL -> (recordId -> Event cache)
     * -- GETTER --
     * Return singleton outer cache
     */
    @Getter
    private final Cache<String, Cache<String, Event>> s3Cache;

    public CacheFactory(S3EnricherProcessorConfig config) {
        this.config = config;
        this.s3Cache = buildS3Cache(); // created ONCE
    }

    /** Build outer cache once */
    private Cache<String, Cache<String, Event>> buildS3Cache() {
        int ttlMinutes = config.getCacheExpirationMinutes();
        int outerMaxSize = 100;

        LOG.info("Initializing SINGLETON S3 URL Cache: maxSize={} ttl={}m",
                outerMaxSize, ttlMinutes);

        return Caffeine.newBuilder()
                .maximumSize(outerMaxSize)
                .expireAfterAccess(ttlMinutes, TimeUnit.MINUTES)
                .recordStats()
                .removalListener((key, value, cause) ->
                        LOG.debug("[Outer S3 URL Eviction] key={} cause={}", key, cause))
                .build();
    }

    /** Inner cache builder (1 per S3 URL) */
    public Cache<String, Event> createEventsCache(String s3Url) {
        int maxSize = config.getCacheSizeLimit();
        int ttlMinutes = config.getCacheExpirationMinutes();

        LOG.info("Creating Inner Events Cache for {}: maxSize={} ttl={}m",
                s3Url, maxSize, ttlMinutes);

        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(ttlMinutes, TimeUnit.MINUTES)
                .recordStats()
                .removalListener((recordId, event, cause) ->
                        LOG.debug("[Inner Event Eviction] s3Url={} recordId={} cause={}",
                                s3Url, recordId, cause))
                .build();
    }

}
