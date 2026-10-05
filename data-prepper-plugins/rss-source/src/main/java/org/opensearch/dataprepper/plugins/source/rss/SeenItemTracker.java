/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rss;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Bounded per-feed deduplication. Tracks recently seen dedup keys, evicting the
 * eldest (by insertion order) once capacity is exceeded. Not durable across
 * restarts (v1). Not thread-safe; assumes single-threaded per-feed access.
 */
class SeenItemTracker {

    private final Set<String> seen;

    SeenItemTracker(final int maxSize) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize must be at least 1, but was " + maxSize);
        }
        final Map<String, Boolean> map = new LinkedHashMap<>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<String, Boolean> eldest) {
                return size() > maxSize;
            }
        };
        this.seen = Collections.newSetFromMap(map);
    }

    /**
     * @return true if the key was not previously seen (and is now recorded),
     *         false if it was already present.
     */
    boolean addIfNew(final String key) {
        return seen.add(key);
    }

    /**
     * @return true if the key is already tracked; does NOT insert it.
     */
    boolean contains(final String key) {
        return seen.contains(key);
    }
}
