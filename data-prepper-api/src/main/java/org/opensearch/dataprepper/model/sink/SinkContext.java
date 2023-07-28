/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import java.util.Collection;
import java.util.List;

/**
 * Data Prepper Sink Context class. This the class for keeping global
 * sink configuration as context so that individual sinks may use them.
 */
public class SinkContext {
    private final String tagsTargetKey;
    private final Collection<String> routes;

    private final List<String> includeKeys;
    private final List<String> excludeKeys;


    public SinkContext(String tagsTargetKey, Collection<String> routes, List<String> includeKeys, List<String> excludeKeys) {
        this.tagsTargetKey = tagsTargetKey;
        this.routes = routes;
        this.includeKeys = includeKeys;
        this.excludeKeys = excludeKeys;
    }

    public SinkContext(String tagsTargetKey) {
        this(tagsTargetKey, null, null, null);
    }

    /**
     * returns the target key name for tags if configured for a given sink
     *
     * @return tags target key
     */
    public String getTagsTargetKey() {
        return tagsTargetKey;
    }

    /**
     * returns routes if configured for a given sink
     *
     * @return routes
     */
    public Collection<String> getRoutes() {
        return routes;
    }

    public List<String> getIncludeKeys() {
        return includeKeys;
    }

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }
}

