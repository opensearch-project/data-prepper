/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.useragent;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import ua_parser.Client;
import ua_parser.Device;
import ua_parser.OS;
import ua_parser.Parser;
import ua_parser.UserAgent;

import java.util.function.Function;

/**
 * A superclass of {@link Parser} which uses Caffeine as a cache.
 */
class CaffeineCachingParser extends Parser {
    private final Cache<String, Client> clientCache;
    private final Cache<String, UserAgent> userAgentCache;
    private final Cache<String, Device> deviceCache;
    private final Cache<String, OS> osCache;

    /**
     * Constructs a new instance with a given cache size. Each parse method
     * will have its own cache.
     *
     * @param cacheSize The size of the cache as a count of items.
     */
    CaffeineCachingParser(final long cacheSize) {
        userAgentCache = createCache(cacheSize);
        clientCache = createCache(cacheSize);
        deviceCache = createCache(cacheSize);
        osCache = createCache(cacheSize);
    }

    @Override
    public Client parse(final String agentString) {
        return parseCaching(agentString, clientCache, super::parse);
    }

    @Override
    public UserAgent parseUserAgent(final String agentString) {
        return parseCaching(agentString, userAgentCache, super::parseUserAgent);
    }

    @Override
    public Device parseDevice(final String agentString) {
        return parseCaching(agentString, deviceCache, super::parseDevice);
    }

    @Override
    public OS parseOS(final String agentString) {
        return parseCaching(agentString, osCache, super::parseOS);
    }

    private <T> T parseCaching(
            final String agentString,
            final Cache<String, T> cache,
            final Function<String, T> parseFunction) {
        if (agentString == null) {
            return null;
        }
        return cache.get(agentString, parseFunction);
    }

    private static <T> Cache<String, T> createCache(final long maximumSize) {
        return Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .build();
    }
}
