/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import java.util.Collection;

/**
 * Data Prepper Sink Context class. This the class for keeping global
 * sink configuration as context so that individual sinks may use them.
 */
public class SinkContext {
    private final String tagsTargetKey;
    private final Collection<String> routes;

    public SinkContext(final String tagsTargetKey, final Collection<String> routes) {
        this.tagsTargetKey = tagsTargetKey;
        this.routes = routes;
    }
    
    /**
     * returns the target key name for tags if configured for a given sink
     * @return tags target key
     */
    public String getTagsTargetKey() {
        return tagsTargetKey;
    }

    /**
     * returns routes if configured for a given sink
     * @return routes
     */
    public Collection<String> getRoutes() {
        return routes;
    }
}

